
/**
 * What shoud we do to change this to asynchronous?
 * 1. Fisrt we need to have a queue to hold all transaction requests.
 * 2. pop next request, send start request for each piece until there is no
 *available left.
 *          in the callback, send the next piece of start request.
 *          if responses to all start requests are collected.
 *              send the finish request
 *                  in callback, remove it from queue.
 *
 *
 */

#include "coord.h"
#include "../frame.h"

namespace rococo {

// TODO obsolete
RequestHeader ThreePhaseCoordinator::gen_header(TxnChopper *ch) {
  RequestHeader header;

  header.cid = coo_id_;
  header.tid = cmd_id_;
  header.t_type = ch->txn_type_;
  return header;
}

/** thread safe */
void ThreePhaseCoordinator::do_one(TxnRequest &req) {
  // pre-process
  std::lock_guard<std::mutex> lock(this->mtx_);
  TxnChopper *ch = Frame().CreateChopper(req);
  cmd_ = ch;
  cmd_id_ = this->next_txn_id();
  cleanup(); // In case of reuse.

  Log::debug("do one request txn_id: %ld\n", cmd_id_);

  if (ccsi_) ccsi_->txn_start_one(thread_id_, ch->txn_type_);

  switch (mode_) {
    case MODE_OCC:
    case MODE_2PL:
      Start();
      break;
    case MODE_RPC_NULL:
//      rpc_null_start(ch);
//      break;
    case MODE_RCC:
    case MODE_RO6:
    case MODE_NONE:
    default:
      verify(0);
  }
  // finish request is triggered in the callback of start request.
}

//void ThreePhaseCoord::rpc_null_start(TxnChopper *ch) {
//  rrr::FutureAttr fuattr;
//
//  fuattr.callback = [ch, this](Future *fu) {
//    ch->reply_.res_ = SUCCESS;
//    TxnReply &txn_reply_buf = ch->get_reply();
//    double last_latency = ch->last_attempt_latency();
//    this->report(txn_reply_buf, last_latency
//#ifdef TXN_STAT
//        , ch
//#endif // ifdef TXN_STAT
//    );
//    ch->callback_(txn_reply_buf);
//    delete ch;
//  };
//
//  RococoProxy *proxy = commo_->vec_rpc_proxy_[coo_id_ % commo_->vec_rpc_proxy_.size()];
//  Future::safe_release(proxy->async_rpc_null(fuattr));
//}

void ThreePhaseCoordinator::IncrementPhaseAndChangeStage(CoordinatorStage stage) {
  phase_++;
  stage_ = stage;
  Log_debug("moving to phase %ld; stage %d", phase_, stage_);
}

bool ThreePhaseCoordinator::IsPhaseOrStageStale(phase_t phase, CoordinatorStage stage) {
  bool result = false;
  if (phase_ != phase) {
    Log_debug("phase %d doesn't match %d\n", phase, phase_);
    result = true;
  }
  if (stage_ != stage) {
    Log_debug("stage %d doesn't match %d\n", stage, stage_);
    result = true;
  }
  return result;
}

void ThreePhaseCoordinator::cleanup() {
  n_start_ = 0;
  n_start_ack_ = 0;
  n_prepare_req_ = 0;
  n_prepare_ack_ = 0;
  n_finish_req_ = 0;
  n_finish_ack_ = 0;
  start_ack_map_.clear();
  IncrementPhaseAndChangeStage(START);
  TxnChopper *ch = (TxnChopper *) cmd_;
}

void ThreePhaseCoordinator::restart(TxnChopper *ch) {
  std::lock_guard<std::mutex> lock(this->mtx_);
  cleanup();
  ch->retry();

  double last_latency = ch->last_attempt_latency();

  if (ccsi_) ccsi_->txn_retry_one(this->thread_id_, ch->txn_type_, last_latency);
  Start();
}

void ThreePhaseCoordinator::Start() {
  std::lock_guard<std::mutex> lock(start_mtx_);
  //  ___TestPhaseOne(cmd_id_);

  if (stage_ != START) {
    IncrementPhaseAndChangeStage(START);
  }

  StartRequest req;
  req.cmd_id = cmd_id_;
  Command *subcmd;

  auto phase = phase_;
  std::function<void(StartReply &)> callback = [this, phase](StartReply &reply) {
    this->StartAck(reply, phase);
    //delete reply.cmd;
  };

  int cnt = 0;
  while ((subcmd = cmd_->GetNextSubCmd()) != nullptr) {
    req.pie_id = next_pie_id();
    n_start_++;
    cnt++;
    Log_debug("send out start request %ld, cmd_id: %lx, inn_id: %d, pie_id: %lx",
              n_start_, cmd_id_, subcmd->inn_id_, req.pie_id);
    req.cmd = subcmd;
    start_ack_map_[subcmd->inn_id()] = false;
    commo_->SendStart(subcmd->GetPar(), req, this, callback);
  }

  Log_debug("sent %d SubCmds\n", cnt);
}

bool ThreePhaseCoordinator::AllStartAckCollected() {
  return std::all_of(start_ack_map_.begin(),
                     start_ack_map_.end(),
                     [](std::pair<innid_t, bool> pair){ return pair.second; });
}

void ThreePhaseCoordinator::StartAck(StartReply &reply, phase_t phase) {
  std::lock_guard<std::mutex> lock(this->mtx_);

  if (IsPhaseOrStageStale(phase, START)) {
    Log_debug("ignore stale startack\n");
    return;
  }
  n_start_ack_++;
  TxnChopper *ch = (TxnChopper *) cmd_;
  start_ack_map_[reply.cmd->inn_id_] = true;

  Log_debug("get start ack %ld/%ld for cmd_id: %lx, inn_id: %d",
            n_start_ack_, n_start_, cmd_id_, reply.cmd->inn_id_);

  if (reply.res == REJECT) {
    Log::debug("got REJECT reply for cmd_id: %lx, inn_id: %d; NOT COMMITING", cmd_id_,reply.cmd->inn_id());
    ch->commit_.store(false);
  }
  if (!ch->commit_.load()) {
    if (n_start_ack_ == n_start_) {
      Log::debug("received all start acks (at least one is REJECT); calling Finish()");
      this->Finish();
    }
  } else {
    cmd_->Merge(*reply.cmd);
    if (cmd_->HasMoreSubCmd()) {
      Log_debug("command has more sub-cmd, cmd_id: %lx,"
                    " n_started_: %d, n_pieces: %d",
                cmd_id_, ch->n_pieces_out_, ch->n_pieces_all_);
      Start();
    } else if (AllStartAckCollected()) {
      Log_debug("receive all start acks, txn_id: %ld; START PREPARE", cmd_id_);
      Prepare();
    }
  }
}

/** caller should be thread_safe */
void ThreePhaseCoordinator::Prepare() {
  IncrementPhaseAndChangeStage(PREPARE);
  TxnChopper *ch = (TxnChopper *) cmd_;
  verify(mode_ == MODE_OCC || mode_ == MODE_2PL);
  // prepare piece, currently only useful for OCC

  auto phase = phase_;
  std::function<void(Future *)> callback = [ch, phase, this](Future *fu) {
    this->PrepareAck(ch, phase, fu);
  };

  std::vector<i32> sids;
  for (auto &rp : ch->partitions_) {
    sids.push_back(rp);
  }

  for (auto &rp : ch->partitions_) {
    RequestHeader header = gen_header(ch);
    Log::debug("send prepare tid: %ld", header.tid);
    commo_->SendPrepare(rp, header.tid, sids, callback);
    site_prepare_[rp]++;
  }
}

void ThreePhaseCoordinator::PrepareAck(TxnChopper *ch, phase_t phase, Future *fu) {
  std::lock_guard<std::mutex> lock(this->mtx_);
  if (IsPhaseOrStageStale(phase, PREPARE)) {
    Log_debug("ignore stale prepareack\n");
    return;
  }

  n_prepare_ack_++;
  int32_t e = fu->get_error_code();

  if (e != 0) {
    ch->commit_.store(false);
    Log_debug("2PL prepare failed due to error");
  } else {
    int res;
    RequestHeader header = gen_header(ch);
    fu->get_reply() >> res;
    Log::debug("tid %ld; prepare result %d", header.tid, res);

    if (res == REJECT) {
      Log::debug("Prepare rejected for %ld by %ld\n", header.tid, ch->inn_id());
      ch->commit_.store(false);
    }
  }

  if (n_prepare_ack_ == ch->partitions_.size()) {
    RequestHeader header = gen_header(ch);
    Log_debug("2PL prepare finished for %ld", header.tid);
    this->Finish();
  }
}

/** caller should be thread safe */
void ThreePhaseCoordinator::Finish() {
  IncrementPhaseAndChangeStage(FINISH);
  ___TestPhaseThree(cmd_id_);
  TxnChopper *ch = (TxnChopper *) cmd_;
  verify(mode_ == MODE_OCC || mode_ == MODE_2PL);
  n_finish_req_++;
  Log_debug("send out finish request, cmd_id: %lx, %ld", cmd_id_, n_finish_req_);

  // commit or abort piece
  auto phase = phase_;
  std::function<void(Future *)> callback = [ch, phase, this](Future *fu) {
    this->FinishAck(ch, phase, fu);
  };

  RequestHeader header = gen_header(ch);
  if (ch->commit_.load()) {
    ch->reply_.res_ = SUCCESS;
    for (auto &rp : ch->partitions_) {
      Log_debug("send commit for txn_id %ld to %ld\n", header.tid, rp);
      commo_->SendCommit(rp, cmd_id_, callback);
      site_commit_[rp]++;
    }
  } else {
    ch->reply_.res_ = REJECT;
    for (auto &rp : ch->partitions_) {
      Log_debug("send abort for txn_id %ld to %ld\n", header.tid, rp);
      commo_->SendAbort(rp, cmd_id_, callback);
      site_abort_[rp]++;
    }
  }
}

void ThreePhaseCoordinator::FinishAck(TxnChopper *ch, phase_t phase, Future *fu) {
  bool callback = false;
  bool retry = false;
  {
    std::lock_guard<std::mutex> lock(this->mtx_);
    if (IsPhaseOrStageStale(phase, FINISH)) {
      Log_debug("ignore stale finish ack\n");
      return;
    }

    n_finish_ack_++;
    Log::debug("finish cmd_id_: %ld; n_finish_ack_: %ld; n_finish_req_: %ld",
               cmd_id_, n_finish_ack_, n_finish_req_);
    verify(ch->GetPars().size() == n_finish_req_);
    if (n_finish_ack_ == ch->GetPars().size()) {
      if ((ch->reply_.res_ == REJECT) && ch->can_retry()) {
        retry = true;
      } else {
        callback = true;
      }
    }
  }
  Log::debug("callback: %s, retry: %s", callback ? "True" : "False",
             retry ? "True" : "False");

  if (retry) {
    this->restart(ch);
  }

  if (callback) {
    // generate a reply and callback.
    TxnReply &txn_reply_buf = ch->get_reply();
    double last_latency = ch->last_attempt_latency();
    this->report(txn_reply_buf, last_latency
#ifdef TXN_STAT
        , ch
#endif // ifdef TXN_STAT
    );

    ch->callback_(txn_reply_buf);
    delete ch;
  }
}

void ThreePhaseCoordinator::report(TxnReply &txn_reply,
                                   double last_latency
#ifdef TXN_STAT
    , TxnChopper *ch
#endif // ifdef TXN_STAT
) {
  if (ccsi_) {
    if (txn_reply.res_ == SUCCESS) {
#ifdef TXN_STAT
      txn_stats_[ch->txn_type_].one(ch->proxies_.size(), ch->p_types_);
#endif // ifdef TXN_STAT
      ccsi_->txn_success_one(thread_id_,
                             txn_reply.txn_type_,
                             txn_reply.start_time_,
                             txn_reply.time_,
                             last_latency,
                             txn_reply.n_try_);
    }
    else
      ccsi_->txn_reject_one(thread_id_,
                            txn_reply.txn_type_,
                            txn_reply.start_time_,
                            txn_reply.time_,
                            last_latency,
                            txn_reply.n_try_);
  }
}

void ThreePhaseCoordinator::___TestPhaseThree(txnid_t txn_id) {
  auto it = ___phase_three_tids_.find(txn_id);
//  verify(it == ___phase_three_tids_.end());
  ___phase_three_tids_.insert(txn_id);
}

void ThreePhaseCoordinator::___TestPhaseOne(txnid_t txn_id) {
  auto it = ___phase_one_tids_.find(txn_id);
  verify(it == ___phase_one_tids_.end());
  ___phase_one_tids_.insert(txn_id);
}


} // namespace rococo
