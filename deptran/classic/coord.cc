
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
#include "../benchmark_control_rpc.h"

namespace rococo {

ClassicCoord::ClassicCoord(uint32_t coo_id,
                                             int benchmark,
                                             ClientControlServiceImpl *ccsi,
                                             uint32_t thread_id)
    : Coordinator(coo_id,
                  benchmark,
                  ccsi,
                  thread_id) {
  // TODO: doesn't belong here;
  // it is currently here so that subclasses such as RCCCoord and OCCoord don't break
  verify(commo_ == nullptr);
//  commo_ = new RococoCommunicator();
//  commo_ = frame_->CreateCommo();
}

RococoCommunicator* ClassicCoord::commo() {
  if (commo_ == nullptr) {
    commo_ = new RococoCommunicator;
  }
  verify(commo_ != nullptr);
  return (RococoCommunicator*) commo_;
}

/** thread safe */
void ClassicCoord::do_one(TxnRequest &req) {
  // pre-process
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  TxnCommand *cmd = frame_->CreateTxnCommand(req, txn_reg_);
  verify(txn_reg_ != nullptr);
  cmd_ = cmd;
  cmd_->root_id_ = this->next_txn_id();
  cmd_->id_ = cmd_->root_id_;
  n_retry_ = 0;
  Reset(); // In case of reuse.

  Log_debug("do one request txn_id: %ld\n", cmd_->id_);

  if (ccsi_) ccsi_->txn_start_one(thread_id_, cmd->type_);

  auto mode = Config::GetConfig()->cc_mode_;
//  verify(mode == MODE_2PL || mode == MODE_OCC || mode == MODE_NONE);
  GotoNextPhase();
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


void ClassicCoord::GotoNextPhase() {
  int n_phase = 4;
  int current_phase = phase_ % n_phase;
  switch (phase_++ % n_phase) {
    case Phase::INIT_END:
      Dispatch();
      verify(phase_ % n_phase == Phase::DISPATCH);
      break;
    case Phase::DISPATCH:
      verify(phase_ % n_phase == Phase::PREPARE);
      verify(!committed_);
      if (aborted_) {
        phase_++;
        Commit();
      } else {
        Prepare();
      }
      break;
    case Phase::PREPARE:
      verify(phase_ % n_phase == Phase::COMMIT);
      Commit();
      break;
    case Phase::COMMIT:
      verify(phase_ % n_phase == Phase::INIT_END);
      if (committed_)
        End();
      else if (aborted_) {
        Restart();
      }
      else
        verify(0);
      break;
    default:
      verify(0);
  }
}

//void ClassicCoord::IncrementPhaseAndChangeStage(CoordinatorStage stage) {
//  phase_++;
//  stage_ = stage;
//  Log_debug("moving to phase %ld; stage %d", phase_, stage_);
//}
//
//bool ClassicCoord::IsPhaseOrStageStale(phase_t phase, CoordinatorStage stage) {
//  bool result = false;
//  if (phase_ != phase) {
//    Log_debug("phase %d doesn't match %d\n", phase, phase_);
//    result = true;
//  }
//  if (stage_ != stage) {
//    Log_debug("stage %d doesn't match %d\n", stage, stage_);
//    result = true;
//  }
//  return result;
//}

void ClassicCoord::Reset() {
  for (int i = 0; i < site_prepare_.size(); i++) {
    site_prepare_[i] = 0;
  }
  n_dispatch_ = 0;
  n_dispatch_ack_ = 0;
  n_prepare_req_ = 0;
  n_prepare_ack_ = 0;
  n_finish_req_ = 0;
  n_finish_ack_ = 0;
  dispatch_acks_.clear();
  aborted_ = false;
  committed_ = false;
//  TxnCommand *ch = (TxnCommand *) cmd_;
}

void ClassicCoord::Restart() {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  verify(aborted_);
  n_retry_++;
  TxnCommand *txn = (TxnCommand*) cmd_;
  double last_latency = txn->last_attempt_latency();
  if (ccsi_)
    ccsi_->txn_retry_one(this->thread_id_, txn->type_, last_latency);
  if (n_retry_ > Config::GetConfig()->max_retry_) {
    End();
  } else {
    Reset();
    txn->Reset();
    GotoNextPhase();
  }
}

void ClassicCoord::Dispatch() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  //  ___TestPhaseOne(cmd_id_);
  auto txn = (TxnCommand*) cmd_;
//  StartRequest req;
//  req.cmd_id = cmd_id_;

  int cnt = 0;
  while (txn->HasMoreSubCmdReadyNotOut()) {
    auto subcmd = (SimpleCommand*) txn->GetNextReadySubCmd();
    subcmd->id_ = next_pie_id();
    verify(subcmd->root_id_ == cmd_->id_);
    n_dispatch_++;
    cnt++;
    Log_debug("send out start request %ld, cmd_id: %lx, inn_id: %d, pie_id: %lx",
              n_dispatch_, cmd_->id_, subcmd->inn_id_, subcmd->id_);
    dispatch_acks_[subcmd->inn_id()] = false;
    commo()->SendDispatch(*subcmd,
                         this,
                         std::bind(&ClassicCoord::DispatchAck,
                                   this,
                                   phase_,
                                   std::placeholders::_1,
                                   std::placeholders::_2));
  }
  Log_debug("sent %d SubCmds\n", cnt);
}

bool ClassicCoord::AllDispatchAcked() {
  bool ret1 = std::all_of(dispatch_acks_.begin(),
                          dispatch_acks_.end(),
                          [] (std::pair<innid_t, bool> pair){
                            return pair.second;
                          });
  if (ret1)
    verify(n_dispatch_ack_ == n_dispatch_);
  return ret1;
}

void ClassicCoord::DispatchAck(phase_t phase, int res, ContainerCommand &cmd) {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  if (phase != phase_) return;
//  if (IsPhaseOrStageStale(phase, HANDOUT)) {
//    Log_debug("ignore stale startack\n");
//    return;
//  }
  n_dispatch_ack_++;
  TxnCommand *ch = (TxnCommand *) cmd_;
  verify(dispatch_acks_[cmd.inn_id_] == false);
  dispatch_acks_[cmd.inn_id_] = true;

  Log_debug("get start ack %ld/%ld for cmd_id: %lx, inn_id: %d",
            n_dispatch_ack_, n_dispatch_, cmd_->id_, cmd.inn_id_);
//  verify(res == SUCCESS);
  if (res == REJECT) {
    Log_debug("got REJECT reply for cmd_id: %lx, inn_id: %d; NOT COMMITING",
               cmd.root_id_, cmd.inn_id());
    aborted_ = true;
    ch->commit_.store(false);
  }
  if (aborted_) {
    if (n_dispatch_ack_ == n_dispatch_) {
      Log_debug("received all start acks (at least one is REJECT); calling "
                    "Finish()");
      GotoNextPhase();
    }
  } else {
    cmd_->Merge(cmd);
    if (cmd_->HasMoreSubCmdReadyNotOut()) {
      Log_debug("command has more sub-cmd, cmd_id: %lx,"
                    " n_started_: %d, n_pieces: %d",
                cmd_->id_, ch->n_pieces_out_, ch->GetNPieceAll());
      Dispatch();
    } else if (AllDispatchAcked()) {
      Log_debug("receive all start acks, txn_id: %ld; START PREPARE", cmd_->id_);
      GotoNextPhase();
    }
  }
}

/** caller should be thread_safe */
void ClassicCoord::Prepare() {
  TxnCommand *cmd = (TxnCommand *) cmd_;
  auto mode = Config::GetConfig()->cc_mode_;
  verify(mode == MODE_OCC || mode == MODE_2PL);

  std::vector<i32> sids;
  for (auto &site : cmd->partition_ids_) {
    sids.push_back(site);
  }

  for (auto &partition_id : cmd->partition_ids_) {
    Log_debug("send prepare tid: %ld; partition_id %d", cmd_->id_, partition_id);
    commo()->SendPrepare(partition_id,
                         cmd_->id_,
                         sids,
                         std::bind(&ClassicCoord::PrepareAck,
                                  this,
                                  phase_,
                                  std::placeholders::_1));
    verify(site_prepare_[partition_id] == 0);
    site_prepare_[partition_id]++;
    verify(site_prepare_[partition_id] == 1);
  }
}

void ClassicCoord::PrepareAck(phase_t phase, Future *fu) {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  if (phase != phase_) return;
  TxnCommand* cmd = (TxnCommand*) cmd_;
  n_prepare_ack_++;
  int32_t e = fu->get_error_code();

  if (e != 0) {
    cmd->commit_.store(false);
    Log_fatal("2PL prepare failed due to error");
  }

  int res;
  fu->get_reply() >> res;
  Log_debug("tid %ld; prepare result %d", cmd_->root_id_, res);

  if (res == REJECT) {
    Log_debug("Prepare rejected for %ld by %ld\n",
               cmd_->root_id_,
               cmd->inn_id());
    cmd->commit_.store(false);
  }

  if (n_prepare_ack_ == cmd->partition_ids_.size()) {
    Log_debug("2PL prepare finished for %ld", cmd->root_id_);
    GotoNextPhase();
  } else {
    // Do nothing.
  }
}

void ClassicCoord::Commit() {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  ___TestPhaseThree(cmd_->id_);
  auto mode = Config::GetConfig()->cc_mode_;
  verify(mode == MODE_OCC || mode == MODE_2PL);
  Log_debug("send out finish request, cmd_id: %lx, %ld",
            txn().id_, n_finish_req_);

  // commit or abort piece
//  RequestHeader header = gen_header(cmd);
  if (txn().commit_.load()) {
    txn().reply_.res_ = SUCCESS;
    for (auto &rp : txn().partition_ids_) {
      n_finish_req_ ++;
      Log_debug("send commit for txn_id %ld to %ld\n", txn().id_, rp);
      commo()->SendCommit(rp,
                         txn().id_,
                          std::bind(&ClassicCoord::CommitAck,
                                    this,
                                    phase_,
                                    std::placeholders::_1));
      site_commit_[rp]++;
    }
  } else {
    txn().reply_.res_ = REJECT;
    for (auto &rp : txn().partition_ids_) {
      n_finish_req_ ++;
      Log_debug("send abort for txn_id %ld to %ld\n", txn().id_, rp);
      commo()->SendAbort(rp,
                        cmd_->id_,
                         std::bind(&ClassicCoord::CommitAck,
                                   this,
                                   phase_,
                                   std::placeholders::_1));
      site_abort_[rp]++;
    }
  }
}

void ClassicCoord::CommitAck(phase_t phase, Future *fu) {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  if (phase != phase_) return;
  TxnCommand* cmd = (TxnCommand*)cmd_;
  n_finish_ack_++;
  Log_debug("finish cmd_id_: %ld; n_finish_ack_: %ld; n_finish_req_: %ld",
            cmd_->id_, n_finish_ack_, n_finish_req_);
  verify(cmd->GetPartitionIds().size() == n_finish_req_);
  if (n_finish_ack_ == cmd->GetPartitionIds().size()) {
    verify(cmd->can_retry());
    if ((cmd->reply_.res_ == REJECT) ) {
      aborted_ = true;
    } else {
      committed_ = true;
    }
    GotoNextPhase();
  }
  Log_debug("callback: %s, retry: %s",
             committed_ ? "True" : "False",
             aborted_ ? "True" : "False");
}

void ClassicCoord::End() {
  TxnCommand* txn = (TxnCommand*) cmd_;
  TxnReply& txn_reply_buf = txn->get_reply();
  double    last_latency  = txn->last_attempt_latency();
  if (committed_) {
    txn->reply_.res_ = SUCCESS;
    this->report(txn_reply_buf, last_latency
#ifdef TXN_STAT
        , txn
#endif // ifdef TXN_STAT
    );
  } else if (aborted_) {
    txn->reply_.res_ = REJECT;
  } else
    verify(0);
  txn->callback_(txn_reply_buf);
  delete txn;

}

void ClassicCoord::report(TxnReply &txn_reply,
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

void ClassicCoord::___TestPhaseThree(txnid_t txn_id) {
  auto it = ___phase_three_tids_.find(txn_id);
//  verify(it == ___phase_three_tids_.end());
  ___phase_three_tids_.insert(txn_id);
}

void ClassicCoord::___TestPhaseOne(txnid_t txn_id) {
  auto it = ___phase_one_tids_.find(txn_id);
  verify(it == ___phase_one_tids_.end());
  ___phase_one_tids_.insert(txn_id);
}


} // namespace rococo
