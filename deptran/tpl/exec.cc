
#include "../config.h"
#include "../multi_value.h"
#include "../rcc/dep_graph.h"
#include "../rcc/graph_marshaler.h"
#include "exec.h"
#include "sched.h"
#include "ps.h"

namespace rococo {
int TPLExecutor::start_launch(
    const RequestHeader &header,
    const map<int32_t, Value> &input,
    const rrr::i32 &output_size,
    rrr::i32 *res,
    map<int32_t, Value> *output,
    rrr::DeferredReply *defer) {
  verify(mdb_txn_ != nullptr);
  verify(mdb_txn_->rtti() == mdb::symbol_t::TXN_2PL);
  verify(phase_ <= 1);

  DragonBall *db = new DragonBall(1, [defer, res]() {
    defer->reply();
  });

  mdb::Txn2PL *txn = (mdb::Txn2PL *) mdb_txn_;
  verify(mdb_txn_ != nullptr);
  if (wounded_) {
    *res = REJECT;
    db->trigger();
  } else {
    init_piece(header.tid,
               header.pid,
               db,
               output);

    Log_debug("get txn handler and start reg lock, txn_id: %lx, pie_id: %lx",
              header.tid, header.pid);
    auto entry = txn_reg_->get(header);
    map<int32_t, Value> no_use;
    entry.txn_handler(this,
                      dtxn_,
                      header,
                      const_cast<map<int32_t, Value>&>(input),
                      res,
                      no_use/*output*/,
                      NULL/*output_size*/);
  }
  return 0;
}

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

    if (ps->can_proceed()) {
      Log::debug("proceed");
      if (ps->is_rejected()) {
        *res = REJECT;

        ps->remove_output();

        Log::debug("rejected");
      } else {
        std::map <int32_t, mdb::Value> *output_vec;
        mdb::Value *output;
        rrr::i32 *output_size;

        ps->get_output(&output_vec, &output, &output_size);

        if (output_vec != NULL) {
          rrr::i32 output_vec_size;
          txn_reg_->get(header).txn_handler(this,
                                            dtxn_,
                                            header,
                                            const_cast<map<int32_t, Value>&>
                                            (input),
                                            res,
                                            *output_vec,
                                            &output_vec_size);
//          output_vec->resize(output_vec_size);
        } else {
          verify(output == nullptr);
          verify(0);
          std::map <int32_t, mdb::Value> no_use;
          txn_reg_->get(header).txn_handler(this,
                                            dtxn_,
                                            header,
                                            const_cast<map<int32_t, Value>&>
                                            (input),
                                            res,
                                            no_use,
                                            output_size);
        }
      }

      Log_debug("set finish on tid %ld\n", header.tid);
      ps->set_finish();
      ps->trigger_reply_dragonball();
    }
  };
}

std::function<void(void)> TPLExecutor::get_2pl_proceed_callback(
    const RequestHeader &header,
    const map<int32_t, Value> &input,
    rrr::i32 *res) {
  return [header, input, res, this]() {
    Log_debug("PROCEED WITH COMMIT: tid: %ld, pid: %ld, p_type: %d, no lock",
              header.tid, header.pid, header.p_type);
//        verify(this->mdb_txn_ != nullptr);
    mdb::Txn2PL *mdb_txn = (mdb::Txn2PL *) mdb_txn_;
    PieceStatus *ps = get_piece_status(header.pid);

    verify(ps != NULL); //FIXME

    if (ps->is_rejected()) {
      // Why is this possibly rejected?
      *res = REJECT;
      ps->remove_output();
      Log::debug("rejected");
    } else {
      map<int32_t, mdb::Value> *output_vec;
      mdb::Value *output;
      rrr::i32 *output_size;

      ps->get_output(&output_vec, &output, &output_size);

      if (output_vec != NULL) {
        rrr::i32 output_vec_size;
        txn_reg_->get(header).txn_handler(this,
                                          dtxn_,
                                          header,
                                          const_cast<map<int32_t, Value>&>
                                          (input),
                                          res,
                                          *output_vec,
                                          &output_vec_size);
//        output_vec->resize(output_vec_size);
      } else {
        verify(output == nullptr);
        verify(0);
        map<int32_t, mdb::Value> no_use;
        txn_reg_->get(header).txn_handler(this,
                                          dtxn_,
                                          header,
                                          const_cast<map<int32_t, Value>&>
                                          (input),
                                          res,
                                          no_use,
                                          output_size);
      }
    }

    ps->set_finish();
    ps->trigger_reply_dragonball();
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
      ps->trigger_reply_dragonball();
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

void TPLExecutor::init_piece(i64 tid,
                             i64 pid,
                             rrr::DragonBall *db,
                             std::map<int32_t, Value> *output) {

  std::function<int(void)> wound_callback =
      [this, tid, pid]() -> int {
        if (this->prepared_) {
          // can't wound
          return 1;
        } else {
//          ((mdb::Txn2PL*)this->mdb_txn_)->wound_ = true;
          this->wounded_ = true;
          return 0;
        }
      };
  PieceStatus *ps =
      new PieceStatus(tid,
                      pid,
                      db,
                      output,
                      &this->wounded_,
                      wound_callback,
                      this,
                      (mdb::Txn2PL*)mdb_txn_);
  piece_map_[pid] = ps;
  SetPsCache(ps);
}


} // namespace rococo
