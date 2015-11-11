#include "all.h"
#include "dtxn_mgr.h"

namespace rococo {

TPLDTxn::TPLDTxn(i64 tid, DTxnMgr *mgr) : DTxn(tid, mgr) {
  verify(mdb_txn_ == nullptr);
  mdb_txn_ = mgr->get_mdb_txn(tid_);
  verify(mdb_txn_ != nullptr);
}

bool TPLDTxn::read_column(
    mdb::Row *row,
    mdb::column_id_t col_id,
    Value *value) {
  verify(mdb_txn_ != nullptr);
  return mdb_txn_->read_column(row, col_id, value);
};

int TPLDTxn::start_launch(
    const RequestHeader &header,
    const std::vector<mdb::Value> &input,
    const rrr::i32 &output_size,
    rrr::i32 *res,
    std::vector<mdb::Value> *output,
    rrr::DeferredReply *defer) {
  if (IS_MODE_2PL) {
    verify(this->mdb_txn_->rtti() == mdb::symbol_t::TXN_2PL);
    DragonBall *defer_reply_db = new DragonBall(1, [defer, res]() {
      defer->reply();
    });
    this->pre_execute_2pl(header, input, res, output, defer_reply_db);

  } else if (IS_MODE_NONE) {
    this->execute(header, input, res, output);
    defer->reply();

  } else if (IS_MODE_OCC) {
    this->execute(header, input, res, output);
    defer->reply();
  } else {
    verify(0);
  }
  return 0;
}

int TPLDTxn::prepare_launch(
    const std::vector<i32> &sids,
    rrr::i32 *res,
    rrr::DeferredReply *defer
) {
  if (Config::GetConfig()->do_logging()) {
    string log_s;
    mgr_->get_prepare_log(tid_, sids, &log_s);
    *res = this->prepare();

    if (*res == SUCCESS)
      recorder_->submit(log_s, [defer]() { defer->reply(); });
    else
      defer->reply();
  } else {
    *res = this->prepare();
    defer->reply();
  }
  return 0;
}

int TPLDTxn::prepare() {
  auto txn = mdb_txn_;
  verify(txn != NULL);
  switch (Config::config_s->mode_) {
    case MODE_OCC:
      if (((mdb::TxnOCC *) txn)->commit_prepare())
        return SUCCESS;
      else
        return REJECT;
    case MODE_2PL: //XXX do logging
      if (((mdb::Txn2PL *) txn)->commit_prepare())
        return SUCCESS;
      else
        return REJECT;
    default:
      verify(0);
  }
  return 0;
}

int TPLDTxn::abort_launch(
    rrr::i32 *res,
    rrr::DeferredReply *defer
) {
  *res = this->abort();
  if (Config::GetConfig()->do_logging()) {
    const char abort_tag = 'a';
    std::string log_s;
    log_s.resize(sizeof(tid_) + sizeof(abort_tag));
    memcpy((void *) log_s.data(), (void *) &tid_, sizeof(tid_));
    memcpy((void *) log_s.data(), (void *) &abort_tag, sizeof(abort_tag));
    recorder_->submit(log_s);
  }
  // TODO optimize
  mgr_->destroy(tid_);
  defer->reply();
  Log::debug("abort finish");
  return 0;
}

int TPLDTxn::abort() {
  verify(mdb_txn_ != NULL);
  verify(mdb_txn_ == mgr_->del_mdb_txn(tid_));
  // TODO fix, might have double delete here.
  mdb_txn_->abort();
  delete mdb_txn_;
  return SUCCESS;
}

int TPLDTxn::commit_launch(
    rrr::i32 *res,
    rrr::DeferredReply *defer
) {
  *res = this->commit();
  if (Config::GetConfig()->do_logging()) {
    const char commit_tag = 'c';
    std::string log_s;
    log_s.resize(sizeof(tid_) + sizeof(commit_tag));
    memcpy((void *) log_s.data(), (void *) &tid_, sizeof(tid_));
    memcpy((void *) log_s.data(), (void *) &commit_tag, sizeof(commit_tag));
    recorder_->submit(log_s);
  }
  mgr_->destroy(tid_);
  defer->reply();
  return 0;
}

int TPLDTxn::commit() {
  verify(mdb_txn_ != NULL);
  verify(mdb_txn_ == mgr_->del_mdb_txn(tid_));
  switch (mgr_->get_mode()) {
    case MODE_OCC:
      ((mdb::TxnOCC *) mdb_txn_)->commit_confirm();
      break;
      //case MODE_DEPTRAN:
    case MODE_2PL:
      mdb_txn_->commit();
      break;
    default:
      verify(0);
  }
  delete mdb_txn_;
  return SUCCESS;
}

std::function<void(void)> TPLDTxn::get_2pl_succ_callback(
    const RequestHeader &header,
    const mdb::Value *input,
    rrr::i32 input_size,
    rrr::i32 *res,
    mdb::Txn2PL::PieceStatus *ps) {
  return [header, input, input_size, res, ps, this]() {
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
      }
      else {
        std::vector<mdb::Value> *output_vec;
        mdb::Value *output;
        rrr::i32 *output_size;

        ps->get_output(&output_vec, &output, &output_size);

        if (output_vec != NULL) {
          rrr::i32 output_vec_size = output_vec->size();
          txn_reg_->get(header).txn_handler(this, header, input,
                                               input_size, res, output_vec->data(),
                                               &output_vec_size, NULL);
          output_vec->resize(output_vec_size);
        }
        else {
          txn_reg_->get(header).txn_handler(this, header, input,
                                               input_size, res, output, output_size, NULL);
        }
      }

      Log_debug("set finish on tid %ld\n", header.tid);
      ps->set_finish();
      ps->trigger_reply_dragonball();
    }
  };
}

std::function<void(void)> TPLDTxn::get_2pl_proceed_callback(
    const RequestHeader &header,
    const mdb::Value *input,
    rrr::i32 input_size,
    rrr::i32 *res) {
  return [header, input, input_size, res, this]() {
    Log_debug("PROCEED WITH COMMIT: tid: %ld, pid: %ld, p_type: %d, no lock",
               header.tid, header.pid, header.p_type);
//        verify(this->mdb_txn_ != nullptr);
    mdb::Txn2PL *mdb_txn = (mdb::Txn2PL *) mdb_txn_;
    mdb::Txn2PL::PieceStatus *ps = mdb_txn->get_piece_status(header.pid);

    verify(ps != NULL); //FIXME

    if (ps->is_rejected()) {
      // Why is this possibly rejected?
      *res = REJECT;
      ps->remove_output();
      Log::debug("rejected");
    }
    else {
      std::vector<mdb::Value> *output_vec;
      mdb::Value *output;
      rrr::i32 *output_size;

      ps->get_output(&output_vec, &output, &output_size);

      if (output_vec != NULL) {
        rrr::i32 output_vec_size = output_vec->size();
        txn_reg_->get(header).txn_handler(this, header, input,
                                             input_size, res, output_vec->data(),
                                             &output_vec_size, NULL);
        output_vec->resize(output_vec_size);
      }
      else {
        txn_reg_->get(header).txn_handler(this, header, input,
                                             input_size, res, output, output_size, NULL);
      }
    }

    ps->set_finish();
    ps->trigger_reply_dragonball();
  };
}

std::function<void(void)> TPLDTxn::get_2pl_fail_callback(
    const RequestHeader &header,
    rrr::i32 *res,
    mdb::Txn2PL::PieceStatus *ps) {
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

void TPLDTxn::pre_execute_2pl(
    const RequestHeader &header,
    const std::vector<mdb::Value> &input,
    rrr::i32 *res,
    std::vector<mdb::Value> *output,
    DragonBall *db
) {
  verify(mdb_txn_ != nullptr);
  mdb::Txn2PL *txn = (mdb::Txn2PL *) mdb_txn_;
  if (txn->is_wound()) {
    output->resize(0);
    *res = REJECT;
    db->trigger();
//    verify(0);
    return;
  }
  txn->init_piece(header.tid, header.pid, db, output);

  Log_debug("get txn handler and start reg lock, txn_id: %lx, pie_id: %lx",
            header.tid, header.pid);
  auto entry = txn_reg_->get(header);
  entry.txn_handler(this, header, input.data(),
                    input.size(), res, NULL/*output*/,
                    NULL/*output_size*/, NULL);
}

} // namespace rococo
