
#include "../config.h"
#include "../multi_value.h"
#include "../rcc/dep_graph.h"
#include "../rcc/graph_marshaler.h"
#include "exec.h"
#include "sched.h"
#include "ps.h"
#include "tpl.h"

namespace rococo {
int TPLExecutor::StartLaunch(
    const RequestHeader &header,
    const map<int32_t, Value> &input,
    const rrr::i32 &output_size,
    rrr::i32 *res,
    map<int32_t, Value> *output,
    rrr::DeferredReply *defer) {
  verify(mdb_txn_ != nullptr);
  verify(mdb_txn_->rtti() == mdb::symbol_t::TXN_2PL);
  verify(phase_ <= 1);


  mdb::Txn2PL *txn = (mdb::Txn2PL *) mdb_txn_;
  verify(mdb_txn_ != nullptr);
  if (wounded_) {
    // other pieces have already been wounded. so cannot proceed.
    *res = REJECT;
    defer->reply();
  } else {
    InitPieceStatus(header,
                    defer,
                    output);

    Log_debug("get txn handler and start reg lock, txn_id: %lx, pie_id: %lx",
              header.tid, header.pid);
    auto entry = txn_reg_->get(header);
    map<int32_t, Value> no_use;


    // pre execute to establish interference.
    TPLDTxn *dtxn = (TPLDTxn*) dtxn_;
    dtxn->locks_.clear();
    dtxn->row_lock_ = nullptr;
    dtxn->locking_ = true;
    entry.txn_handler(this,
                      dtxn_,
                      header,
                      const_cast<map<int32_t, Value>&>(input),
                      res,
                      no_use/*output*/);
    // try to require all the locks.
    dtxn->locking_ = false;
    PieceStatus *ps = get_piece_status(header.pid);
    verify(ps_cache_ == ps);
    auto succ_callback = get_2pl_succ_callback(header, input, res, ps);
    if (dtxn->locks_.size() > 0) {
      auto fail_callback = get_2pl_fail_callback(header, res, ps);
      ps->reg_rw_lock(dtxn->locks_, succ_callback, fail_callback);
    } else if (dtxn->row_lock_ != nullptr) {
      auto fail_callback = get_2pl_fail_callback(header, res, ps);
      ps->reg_rm_lock(dtxn->row_lock_, succ_callback, fail_callback);
    } else {
      succ_callback();
    }
  }
  return 0;
}

//int TPLExecutor::
//int TPLExecutor::StartLock() {
//
//}

std::function<void(void)> TPLExecutor::get_2pl_succ_callback(
    const RequestHeader &header,
    const map<int32_t, Value> &input,
    rrr::i32 *res,
    PieceStatus *ps) {
  return [header, input, res, ps, this]() {
    Log_debug("lock acquired call back, txn_id: %lx, pie_id: %lx, p_type: %d, ",
              header.tid, header.pid, header.p_type);
    Log_debug("succ 1 callback: PS: %p", ps);
    verify(ps != NULL);
    ps->start_yes_callback();
    Log_debug("tid: %lx, pid: %lx, p_type: %d, get lock",
              header.tid, header.pid, header.p_type);
    Log::debug("proceed");
    ((TPLDTxn*)dtxn_)->locking_ = false;
    if (ps->is_rejected()) {
      *res = REJECT;
      ps->remove_output();
      Log::debug("rejected");
    } else {
      std::map <int32_t, mdb::Value> *output;
      mdb::Value *output_value;
      rrr::i32 *output_size;
      ps->get_output(&output, &output_value, &output_size);
      output->clear();
      txn_reg_->get(header).txn_handler(this,
                                        dtxn_,
                                        header,
                                        const_cast<map<int32_t, Value>&>
                                        (input),
                                        res,
                                        *output);
      verify(*res == SUCCESS);
    }

    Log_debug("set finish on tid %ld\n", header.tid);
    ps->set_finish();
    ps->defer_->reply();
  };
}

std::function<void(void)> TPLExecutor::get_2pl_fail_callback(
    const RequestHeader &header,
    rrr::i32 *res,
    PieceStatus *ps) {
  return [header, res, ps, this]() {
    Log_debug("tid: %ld, pid: %ld, p_type: %d, lock timeout call back",
              header.tid, header.pid, header.p_type);
    //PieceStatus *ps = TPL::get_piece_status(header);
    Log::debug("fail callback: PS: %p", ps);
    verify(ps != NULL); //FIXME
//    ((mdb::Txn2PL*)this->mdb_txn_)->ps_cache_ = ps;
    ps->start_no_callback();

    if (ps->can_proceed()) {
      *res = REJECT;

      ps->remove_output();

      ps->set_finish();
      ps->defer_->reply();
    }
  };
}


int TPLExecutor::prepare() {
  auto txn = (mdb::Txn2PL *) mdb_txn_;
  verify(txn != NULL);
//  verify(Config::config_s->mode_ == MODE_2PL);

  prepared_ = true;
  if (!wounded_) {
    return SUCCESS;
  } else {
    return REJECT;
  }
}


int TPLExecutor::commit() {
  verify(mdb_txn_ != nullptr);
  verify(mdb_txn_ == sched_->RemoveMTxn(cmd_id_));
  mdb_txn_->commit();
  delete mdb_txn_;
  mdb_txn_ = nullptr;
  release_piece_map(true/*commit*/);
  return SUCCESS;
}

int TPLExecutor::abort() {
  ThreePhaseExecutor::abort();
  release_piece_map(false);
  return 0;
}

PieceStatus* TPLExecutor::ps_cache() {
//  verify(ps_cache_ == ps_cache_s);
  return ps_cache_;
}

void TPLExecutor::SetPsCache(PieceStatus* ps) {
  ps_cache_ = ps;
}


void TPLExecutor::release_piece_map(bool commit) {
  verify(piece_map_.size() != 0);
  SetPsCache(nullptr);
  if (commit) {
    for (auto &it : piece_map_) {
      it.second->commit();
      delete it.second;
    }
    piece_map_.clear();
  }
  else {
    for (auto &it : piece_map_) {
      it.second->abort();
      delete it.second;
    }
    piece_map_.clear();
  }
}

PieceStatus* TPLExecutor::get_piece_status(i64 pid) {
  auto ps = ps_cache();
  if (ps == nullptr || ps->pid_ != pid) {
    verify(piece_map_.find(pid) != piece_map_.end());
    ps = piece_map_[pid];
    SetPsCache(ps);
  }
  return ps;
}

void TPLExecutor::InitPieceStatus(const RequestHeader &header,
                                  rrr::DeferredReply* defer,
                                  std::map<int32_t, Value> *output) {

  auto tid = header.tid;
  auto pid = header.pid;
  std::function<int(void)> wound_callback =
      [this, tid, pid]() -> int {
        if (this->prepared_) {
          // can't wound
          return 1;
        } else {
          this->wounded_ = true;
          return 0;
        }
      };
  PieceStatus *ps =
      new PieceStatus(header,
                      defer,
                      output,
//                      &this->wounded_,
                      wound_callback,
                      this);
  piece_map_[pid] = ps;
  SetPsCache(ps);
}

} // namespace rococo
