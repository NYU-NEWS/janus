
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
  verify(commo_ == nullptr);
}

RococoCommunicator* ClassicCoord::commo() {
  if (commo_ == nullptr) {
    commo_ = new RococoCommunicator;
  }
  verify(commo_ != nullptr);
  return (RococoCommunicator*) commo_;
}

void ClassicCoord::ForwardTxnRequest(TxnRequest &req) {
  auto comm = commo();
  comm->SendForwardTxnRequest(
      req,
      this,
      std::bind(&ClassicCoord::ForwardTxnRequestAck,
                this,
                std::placeholders::_1
      ));
}


void ClassicCoord::ForwardTxnRequestAck(const TxnReply& txn_reply) {
  Log_info("%s: %d", __FUNCTION__, txn_reply.res_);
  committed_ = (txn_reply.res_ == REJECT) ? false : true;
  aborted_ = !committed_;
  phase_ = Phase::COMMIT;
  GotoNextPhase();
}

void ClassicCoord::do_one(TxnRequest &req) {
    std::lock_guard<std::recursive_mutex> lock(this->mtx_);
    TxnCommand *cmd = frame_->CreateTxnCommand(req, txn_reg_);
    verify(txn_reg_ != nullptr);
    cmd->root_id_ = this->next_txn_id();
    cmd->id_ = cmd->root_id_;
    cmd->timestamp_ = cmd->root_id_;
    cmd_ = cmd;
    n_retry_ = 0;
    Reset(); // In case of reuse.

    Log_debug("do one request txn_id: %d", cmd_->id_);
    auto config = Config::GetConfig();
    bool not_forwarding = forward_status_ != PROCESS_FORWARD_REQUEST;

    if (ccsi_ && not_forwarding) {
      ccsi_->txn_start_one(thread_id_, cmd->type_);
    }
    if (config->forwarding_enabled_ && forward_status_ == FORWARD_TO_LEADER) {
      Log_info("forward to leader: %d; cooid: %d", forward_status_, this->coo_id_);
      ForwardTxnRequest(req);
    } else {
      Log_info("start txn!!! : %d", forward_status_);
      GotoNextPhase();
    }
}

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
      } else
        verify(0);
      break;
    default:
      verify(0);
  }
}

void ClassicCoord::Reset() {
  Coordinator::Reset();
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
  committed_ = false;
  aborted_ = false;
}

void ClassicCoord::Restart() {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  verify(aborted_);
  n_retry_++;
  cmd_->root_id_ = this->next_txn_id();
  cmd_->id_ = cmd_->root_id_;
  TxnCommand *txn = (TxnCommand*) cmd_;
  double last_latency = txn->last_attempt_latency();
  if (ccsi_)
    ccsi_->txn_retry_one(this->thread_id_, txn->type_, last_latency);
  auto& max_retry = Config::GetConfig()->max_retry_;
  if (n_retry_ > max_retry && max_retry >= 0) {
    if (ccsi_)
      ccsi_->txn_give_up_one(this->thread_id_, txn->type_);
    End();
  } else {
    // Log_info("retry count %d, max_retry: %d, this coord: %llx", n_retry_, max_retry, this);
    Reset();
    txn->Reset();
    GotoNextPhase();
  }
}

void ClassicCoord::Dispatch() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto txn = (TxnCommand*) cmd_;

  int cnt = 0;
  auto n_pd = Config::GetConfig()->n_parallel_dispatch_;
  n_pd = 0;
  auto cmds_by_par = txn->GetReadyCmds(n_pd);
  for (auto& pair: cmds_by_par) {
    const parid_t& par_id = pair.first;
    vector<SimpleCommand*>& cmds = pair.second;
    n_dispatch_ += cmds.size();
    cnt += cmds.size();
    vector<SimpleCommand> cc;
    for (SimpleCommand* c: cmds) {
      c->id_ = next_pie_id();
      dispatch_acks_[c->inn_id_] = false;
      cc.push_back(*c);
    }
    commo()->SendDispatch(cc,
                          this,
                          std::bind(&ClassicCoord::DispatchAck,
                                    this,
                                    phase_,
                                    std::placeholders::_1,
                                    std::placeholders::_2));
  }
  Log_debug("sent %d subcmds\n", cnt);
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

void ClassicCoord::DispatchAck(phase_t phase,
                               int res,
                               TxnOutput &outputs) {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  if (phase != phase_) return;
  TxnCommand *txn = (TxnCommand *) cmd_;
  if (res == REJECT) {
    Log_debug("got REJECT reply for cmd_id: %llx NOT COMMITING",
              txn->root_id_);
    aborted_ = true;
    txn->commit_.store(false);
  }
  n_dispatch_ack_ += outputs.size();
  if (aborted_) {
    if (n_dispatch_ack_ == n_dispatch_) {
      Log_debug("received all start acks (at least one is REJECT); calling "
                    "Finish()");
      GotoNextPhase();
      return;
    }
  } else {
    for (auto& pair : outputs) {
      const innid_t& inn_id = pair.first;
      verify(dispatch_acks_.at(inn_id) == false);
      dispatch_acks_[inn_id] = true;
      Log_debug("get start ack %ld/%ld for cmd_id: %lx, inn_id: %d",
                n_dispatch_ack_, n_dispatch_, cmd_->id_, inn_id);
      txn->Merge(pair.first, pair.second);
    }
    if (txn->HasMoreSubCmdReadyNotOut()) {
      Log_debug("command has more sub-cmd, cmd_id: %llx,"
                    " n_started_: %d, n_pieces: %d",
                txn->id_, txn->n_pieces_dispatched_, txn->GetNPieceAll());
      Dispatch();
    } else if (AllDispatchAcked()) {
      Log_debug("receive all start acks, txn_id: %llx; START PREPARE",
                txn->id_);
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

void ClassicCoord::PrepareAck(phase_t phase, int res) {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  if (phase != phase_) return;
  TxnCommand* cmd = (TxnCommand*) cmd_;
  n_prepare_ack_++;

  if (res == REJECT) {
    cmd->commit_.store(false);
    aborted_ = true;
//    Log_fatal("2PL prepare failed due to error %d", e);
  }
  Log_debug("tid %llx; prepare result %d", (int64_t)cmd_->root_id_, res);

  if (n_prepare_ack_ == cmd->partition_ids_.size()) {
    Log_debug("2PL prepare finished for %ld", cmd->root_id_);
    if (!aborted_) {
      cmd->commit_.store(true);
      committed_ = true;
    }
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

  verify(txn().commit_.load() == committed_);

  if (committed_) {
    txn().reply_.res_ = SUCCESS;
    for (auto &rp : txn().partition_ids_) {
      n_finish_req_ ++;
      Log_debug("send commit for txn_id %ld to %ld\n", txn().id_, rp);
      commo()->SendCommit(rp,
                         txn().id_,
                          std::bind(&ClassicCoord::CommitAck,
                                    this,
                                    phase_));
      site_commit_[rp]++;
    }
  } else if (aborted_) {
    txn().reply_.res_ = REJECT;
    for (auto &rp : txn().partition_ids_) {
      n_finish_req_ ++;
      Log_debug("send abort for txn_id %ld to %ld\n", txn().id_, rp);
      commo()->SendAbort(rp,
                        cmd_->id_,
                         std::bind(&ClassicCoord::CommitAck,
                                   this,
                                   phase_));
      site_abort_[rp]++;
    }
  } else {
    verify(0);
  }
  GotoNextPhase();
}

void ClassicCoord::CommitAck(phase_t phase) {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  if (phase != phase_) return;
  TxnCommand* cmd = (TxnCommand*)cmd_;
  n_finish_ack_++;
  Log_debug("finish cmd_id_: %ld; n_finish_ack_: %ld; n_finish_req_: %ld",
            cmd_->id_, n_finish_ack_, n_finish_req_);
  verify(cmd->GetPartitionIds().size() == n_finish_req_);
  if (n_finish_ack_ == cmd->GetPartitionIds().size()) {
    if ((cmd->reply_.res_ == REJECT) ) {
      aborted_ = true;
    } else {
      committed_ = true;
    }
  }
  Log_debug("callback: %s, retry: %s",
             committed_ ? "True" : "False",
             aborted_ ? "True" : "False");
}

void ClassicCoord::End() {
  TxnCommand* txn = (TxnCommand*) cmd_;
  TxnReply& txn_reply_buf = txn->get_reply();
  double last_latency  = txn->last_attempt_latency();
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

  bool not_forwarding = forward_status_ != PROCESS_FORWARD_REQUEST;
  if (ccsi_ && not_forwarding) {
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
    } else
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
