
#include "../config.h"
#include "../multi_value.h"
#include "../txn_chopper.h"
#include "../rcc/dep_graph.h"
#include "../rcc/graph_marshaler.h"
#include "exec.h"
#include "sched.h"
#include "ps.h"
#include "tpl.h"

namespace rococo {

int TplExecutor::OnDispatch(const SimpleCommand &cmd,
                            rrr::i32 *res,
                            map<int32_t, Value> *output,
                            const function<void()> &callback) {
  verify(mdb_txn()->rtti() == mdb::symbol_t::TXN_2PL);
  verify(phase_ <= 1);
  mdb::Txn2PL *txn = (mdb::Txn2PL *) mdb_txn();
  verify(mdb_txn_ != nullptr);
  if (wounded_) {
    // other pieces have already been wounded. so cannot proceed.
    Log_debug("wounded");
    *res = REJECT;
    callback();
  } else {
    InitPieceStatus(cmd, callback, output);
    Log_debug("get txn handler and start reg lock, txn_id: %lx, pie_id: %lx",
              cmd.root_id_, cmd.id_);
    auto entry = txn_reg_->get(cmd);
    map<int32_t, Value> no_use;
    // pre execute to establish interference.
    TPLDTxn *dtxn = (TPLDTxn*) dtxn_;
    dtxn->locks_.clear();
    dtxn->row_lock_ = nullptr;
    dtxn->locking_ = true;
    entry.txn_handler(this,
                      dtxn_,
                      const_cast<SimpleCommand&>(cmd),
                      res,
                      no_use/*output*/);
    // try to require all the locks.
    dtxn->locking_ = false;
    PieceStatus *ps = get_piece_status(cmd.id_);
    verify(ps_cache_ == ps);
    auto succ_callback = std::bind(&TplExecutor::LockSucceeded,
                                   this, cmd, res, ps);
    auto fail_callback = std::bind(&TplExecutor::LockFailed,
                                   this, cmd, res, ps);
    if (dtxn->locks_.size() > 0) {
      if (cmd.type_ == 1000) {
        Log_debug("lock size for tpcc new order piece 1000, %d",
                 dtxn->locks_.size());
      }
      ps->reg_rw_lock(dtxn->locks_, succ_callback, fail_callback);
    } else if (dtxn->row_lock_ != nullptr) {
      ps->reg_rm_lock(dtxn->row_lock_, succ_callback, fail_callback);
    } else {
      succ_callback();
    }
  }
  return 0;
}

void TplExecutor::LockSucceeded(const SimpleCommand& cmd,
                                rrr::i32 *res,
                                PieceStatus *ps) {
  Log_debug("lock acquired call back, txn_id: %lx, pie_id: %lx, p_type: %d, ",
            cmd.root_id_, cmd.id_, cmd.type_);
  Log_debug("succ 1 callback: PS: %p", ps);
  verify(ps != NULL);
  ps->start_yes_callback();
  TPLDTxn *dtxn = (TPLDTxn*) dtxn_;
  Log_debug("tid: %lx, pid: %lx, p_type: %d, get lock",
            cmd.root_id_, cmd.id_, cmd.type_);
  Log::debug("proceed");
  dtxn->locking_ = false;
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
    txn_reg_->get(cmd).txn_handler(this,
                                   dtxn_,
                                   const_cast<SimpleCommand&>(cmd),
                                   res,
                                   *output);
    verify(*res == SUCCESS);
    // ____debug purpose
    for (auto &kv : *output) {
      auto &v = kv.second;
      auto k = v.get_kind();
      if (k == Value::I32 || k == Value::I64
          || k == Value::STR || k == Value::DOUBLE) {

      } else {
        Log_fatal("xxx: %d", cmd.type_);
        verify(0);
      }
    }
  }

  Log_debug("set finish on tid %ld\n", cmd.root_id_);
  ps->set_finish();
  ps->callback_();
}

void TplExecutor::LockFailed(const SimpleCommand& cmd,
                             rrr::i32 *res,
                             PieceStatus *ps) {
  Log_debug("tid: %llx, pid: %llx, p_type: %d, lock reject call back",
           (int64_t)cmd.root_id_, (int64_t)cmd.id_, (int)cmd.type_);
  Log::debug("fail callback: PS: %p", ps);
  verify(ps != NULL); //FIXME
  ps->start_no_callback();

  if (ps->can_proceed()) {
    *res = REJECT;
    ps->remove_output();
    ps->set_finish();
    ps->callback_();
  }
}

bool TplExecutor::Prepare() {
  auto txn = (mdb::Txn2PL *) mdb_txn();
  verify(txn != NULL);
  if (wounded_)
    Log_debug("wounded");
  prepared_ = true;
  return !wounded_;
}


int TplExecutor::Commit() {
  verify(mdb_txn_ != nullptr);
  verify(mdb_txn_ == sched_->RemoveMTxn(cmd_id_));
  mdb_txn_->commit();
  delete mdb_txn_;
  mdb_txn_ = nullptr;
  release_piece_map(true/*commit*/);
  return SUCCESS;
}

int TplExecutor::Abort() {
  ClassicExecutor::Abort();
  release_piece_map(false);
  return 0;
}

PieceStatus* TplExecutor::ps_cache() {
  return ps_cache_;
}

void TplExecutor::SetPsCache(PieceStatus* ps) {
  ps_cache_ = ps;
}


void TplExecutor::release_piece_map(bool commit) {
  // TODO FIXME
  // verify(piece_map_.size() != 0);
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

PieceStatus* TplExecutor::get_piece_status(i64 pid) {
  auto ps = ps_cache();
  if (ps == nullptr || ps->pid_ != pid) {
    verify(piece_map_.find(pid) != piece_map_.end());
    ps = piece_map_[pid];
    SetPsCache(ps);
  }
  return ps;
}

void TplExecutor::InitPieceStatus(const SimpleCommand &cmd,
                                  const function<void()>& callback,
                                  map<int32_t, Value> *output) {

  auto tid = cmd.root_id_;
  auto pid = cmd.id_;
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
  PieceStatus *ps = new PieceStatus(cmd,
                                    callback,
                                    output,
                                    wound_callback,
                                    this);
  piece_map_[pid] = ps;
  SetPsCache(ps);
}

} // namespace rococo
