
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
 */

#include "coordinator.h"
#include "../frame.h"
#include "../benchmark_control_rpc.h"
#include "bench/spanner/workload.h"
#include "bench/dynamic/workload.h"

namespace janus {

CoordinatorClassic::CoordinatorClassic(uint32_t coo_id,
                                       int benchmark,
                                       ClientControlServiceImpl* ccsi,
                                       uint32_t thread_id)
    : Coordinator(coo_id,
                  benchmark,
                  ccsi,
                  thread_id) {
  verify(commo_ == nullptr);
}

Communicator* CoordinatorClassic::commo() {
  if (commo_ == nullptr) {
    commo_ = new Communicator;
  }
  verify(commo_ != nullptr);
  return commo_;
}

void CoordinatorClassic::ForwardTxnRequest(TxRequest& req) {
  auto comm = commo();
  comm->SendForwardTxnRequest(
      req,
      this,
      std::bind(&CoordinatorClassic::ForwardTxRequestAck,
                this,
                std::placeholders::_1
      ));
}

void CoordinatorClassic::ForwardTxRequestAck(const TxReply& txn_reply) {
  Log_info("%s: %d", __FUNCTION__, txn_reply.res_);
  committed_ = (txn_reply.res_ == REJECT) ? false : true;
  aborted_ = !committed_;
  phase_ = Phase::COMMIT;
  GotoNextPhase();
}

void CoordinatorClassic::DoTxAsync(TxRequest& req) {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  TxData* cmd = frame_->CreateTxnCommand(req, txn_reg_);
  verify(txn_reg_ != nullptr);
  cmd->root_id_ = this->next_txn_id();
  cmd->id_ = cmd->root_id_;
  ongoing_tx_id_ = cmd->id_;
  Log_debug("assigning tx id: %" PRIx64, ongoing_tx_id_);
  cmd->timestamp_ = GenerateTimestamp();
  cmd_ = cmd;
  n_retry_ = 0;
    // reset LFC related
    n_ssid_consistent_ = 0;
    n_decided_ = 0;
    // int n_offset_valid_ = 0;
    n_validation_passed = 0;
    n_cascading_aborts = 0;
    n_rotxn_aborts = 0;
    n_early_aborts = 0;
    // int n_single_shard = 0;
    // int n_single_shard_write_only = 0;
  Reset(); // In case of reuse.

  Log_debug("do one request txn_id: %d", cmd_->id_);
  auto config = Config::GetConfig();
  bool not_forwarding = forward_status_ != PROCESS_FORWARD_REQUEST;

  if (ccsi_ && not_forwarding) {
    ccsi_->txn_start_one(thread_id_, cmd->type_);
  }
  if (config->forwarding_enabled_ && forward_status_ == FORWARD_TO_LEADER) {
    Log_info("forward to leader: %d; cooid: %d",
             forward_status_,
             this->coo_id_);
    verify(0); // not supported yet for the new open closed loop.
    ForwardTxnRequest(req);
  } else {
    Log_debug("start txn!!! : %d", forward_status_);
    Coroutine::CreateRun([this]() { GotoNextPhase(); });
  }
}

void CoordinatorClassic::GotoNextPhase() {
  int n_phase = 4;
  auto current_phase = phase_ % n_phase;
  phase_++;
  switch (current_phase) {
    case Phase::INIT_END:
      DispatchAsync();
      verify(phase_ % n_phase == Phase::DISPATCH);
      break;
    case Phase::DISPATCH:
      verify(phase_ % n_phase == Phase::PREPARE);
      verify(!committed_);
//      if (aborted_) {
//        phase_++;
//        Commit();
//      } else {
        Prepare();
//      }
      break;
    case Phase::PREPARE:
      verify(phase_ % n_phase == Phase::COMMIT);
      Commit();
      break;
    case Phase::COMMIT:
      verify(phase_ % n_phase == Phase::INIT_END);
      verify(committed_ != aborted_);
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

void CoordinatorClassic::Reset() {
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

void CoordinatorClassic::Restart() {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  verify(aborted_);
  n_retry_++;
  cmd_->root_id_ = this->next_txn_id();
  cmd_->id_ = cmd_->root_id_;
  ongoing_tx_id_ = cmd_->root_id_;
  Log_debug("assigning tx_id: %" PRIx64, ongoing_tx_id_);
  TxData* txn = (TxData*) cmd_;
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

void CoordinatorClassic::DispatchAsync() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto txn = (TxData*) cmd_;

  int cnt = 0;
  //auto n_pd = Config::GetConfig()->n_parallel_dispatch_;
  //n_pd = 1;
  //auto n_pd = 0; //  all ready pieces sent in one parallel round
  //auto cmds_by_par = txn->GetReadyPiecesData(n_pd);
    /*
    int pieces_per_hop = 0;
    if (cmd_->type_ == SPANNER_RW && tx_data().n_pieces_dispatch_acked_ == 0) {
        // we make spanner RW txn multi-hop!
        pieces_per_hop = cmd_->spanner_rw_reads;
    }
    if (cmd_->type_ == DYNAMIC_RW && tx_data().n_pieces_dispatch_acked_ == 0) {
        // we make dynamic RW txn multi-hop!
        pieces_per_hop = cmd_->dynamic_rw_reads;
    }
    */
  //  auto cmds_by_par = txn->GetReadyPiecesData(pieces_per_hop); // all ready pieces sent in one parallel round
  auto cmds_by_par = txn->GetReadyPiecesData(); //all ready pieces sent in one parallel round
  Log_debug("Dispatch for tx_id: %" PRIx64, txn->root_id_);
  for (auto& pair: cmds_by_par) {
    const parid_t& par_id = pair.first;
    auto& cmds = pair.second;
    n_dispatch_ += cmds.size();
    cnt += cmds.size();
    auto sp_vec_piece = std::make_shared<vector<shared_ptr<TxPieceData>>>();
    for (auto c: cmds) {
      c->id_ = next_pie_id();
      dispatch_acks_[c->inn_id_] = false;
      sp_vec_piece->push_back(c);
    }
    commo()->BroadcastDispatch(this->coo_id_, sp_vec_piece,
                               this,
                               std::bind(&CoordinatorClassic::DispatchAck,
                                         this,
                                         phase_,
                                         std::placeholders::_1,
                                         std::placeholders::_2));
  }
  Log_debug("Dispatch cnt: %d for tx_id: %" PRIx64, cnt, txn->root_id_);
}

bool CoordinatorClassic::AllDispatchAcked() {
  bool ret1 = std::all_of(dispatch_acks_.begin(),
                          dispatch_acks_.end(),
                          [](std::pair<innid_t, bool> pair) {
                            return pair.second;
                          });
  if (ret1)
    verify(n_dispatch_ack_ == n_dispatch_);
  return ret1;
}

void CoordinatorClassic::DispatchAck(phase_t phase,
                                     int res,
                                     TxnOutput& outputs) {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  if (phase != phase_) return;
  auto* txn = (TxData*) cmd_;
  if (res == REJECT) {
    Log_debug("got REJECT reply for cmd_id: %llx NOT COMMITING",
              txn->root_id_);
    aborted_ = true;
    txn->commit_.store(false);
  }
  n_dispatch_ack_ += outputs.size();
//  if (aborted_) {
//    if (n_dispatch_ack_ == n_dispatch_) {
//      // wait until all ongoing dispatch to finish before aborting.
//      Log_debug("received all start acks (at least one is REJECT);"
//                    "tx_id: %"
//                    PRIx64, txn->root_id_);
//      GotoNextPhase();
//      return;
//    }
//  }

  for (auto& pair : outputs) {
    const innid_t& inn_id = pair.first;
    verify(!dispatch_acks_.at(inn_id));
    dispatch_acks_[inn_id] = true;
    Log_debug("get start ack %ld/%ld for cmd_id: %lx, inn_id: %d",
              n_dispatch_ack_, n_dispatch_, cmd_->id_, inn_id);
    txn->Merge(pair.first, pair.second);
  }
  if (txn->HasMoreUnsentPiece()) {
    Log_debug("command has more sub-cmd, cmd_id: %llx,"
                  " n_started_: %d, n_pieces: %d",
              txn->id_, txn->n_pieces_dispatched_, txn->GetNPieceAll());
    DispatchAsync();
  } else if (AllDispatchAcked()) {
    Log_debug("receive all start acks, txn_id: %llx; START PREPARE",
              txn->id_);
    GotoNextPhase();
  }
}

/** caller should be thread_safe */
void CoordinatorClassic::Prepare() {
  TxData* cmd = (TxData*) cmd_;
  auto mode = Config::GetConfig()->tx_proto_;
  verify(mode == MODE_OCC || mode == MODE_2PL);

  std::vector<i32> sids;
  for (auto& site : cmd->partition_ids_) {
    sids.push_back(site);
  }

  for (auto& partition_id : cmd->partition_ids_) {
    Log_debug("send prepare tid: %ld; partition_id %d",
              cmd_->id_,
              partition_id);
    commo()->SendPrepare(partition_id,
                         cmd_->id_,
                         sids,
                         std::bind(&CoordinatorClassic::PrepareAck,
                                   this,
                                   phase_,
                                   std::placeholders::_1));
    verify(site_prepare_[partition_id] == 0);
    site_prepare_[partition_id]++;
    verify(site_prepare_[partition_id] == 1);
  }
}

void CoordinatorClassic::PrepareAck(phase_t phase, int res) {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  if (phase != phase_) return;
  TxData* cmd = (TxData*) cmd_;
  n_prepare_ack_++;

  verify(res == SUCCESS || res == REJECT);
  if (res == REJECT) {
    cmd->commit_.store(false);
    aborted_ = true;
//    Log_fatal("2PL prepare failed due to error %d", e);
  }
  Log_debug("tid %llx; prepare result %d", (int64_t) cmd_->root_id_, res);

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

void CoordinatorClassic::Commit() {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
//  ___TestPhaseThree(cmd_->id_);
  auto mode = Config::GetConfig()->tx_proto_;
  verify(mode == MODE_OCC || mode == MODE_2PL);
  Log_debug("send out finish request, cmd_id: %"
                PRIx64
                ", %d",
            tx_data().id_, n_finish_req_);

  verify(tx_data().commit_.load() == committed_);
  verify(committed_ != aborted_);
  if (committed_) {
    tx_data().reply_.res_ = SUCCESS;
    for (auto& rp : tx_data().partition_ids_) {
      n_finish_req_++;
      Log_debug("send commit for txn_id %"
                    PRIx64
                    " to %d", tx_data().id_, rp);
      commo()->SendCommit(rp,
                          tx_data().id_,
                          std::bind(&CoordinatorClassic::CommitAck,
                                    this,
                                    phase_));
      site_commit_[rp]++;
    }
  } else if (aborted_) {
    tx_data().reply_.res_ = REJECT;
    for (auto& rp : tx_data().partition_ids_) {
      n_finish_req_++;
      Log_debug("send abort for txn_id %"
                    PRIx64
                    " to %d", tx_data().id_, rp);
      commo()->SendAbort(rp,
                         cmd_->id_,
                         std::bind(&CoordinatorClassic::CommitAck,
                                   this,
                                   phase_));
      site_abort_[rp]++;
    }
  } else {
    verify(0);
  }
}

void CoordinatorClassic::CommitAck(phase_t phase) {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  // TODO fix bug: when receiving a reply, the coordinator already frees.
  if (phase != phase_) return;
  TxData* cmd = (TxData*) cmd_;
  n_finish_ack_++;
  Log_debug("finish cmd_id_: %ld; n_finish_ack_: %ld; n_finish_req_: %ld",
            cmd_->id_, n_finish_ack_, n_finish_req_);
  verify(cmd->GetPartitionIds().size() == n_finish_req_);
  // Perhaps a bug here?
  if (n_finish_ack_ == cmd->GetPartitionIds().size()) {
//    if (cmd->reply_.res_ == REJECT) {
//      aborted_ = true;
//    } else {
//      committed_ = true;
//    }
    GotoNextPhase();
  }
  Log_debug("callback: %s, retry: %s",
            committed_ ? "True" : "False",
            aborted_ ? "True" : "False");
}

void CoordinatorClassic::ReportCommit() {
  auto* tx_data = (TxData*) cmd_;
  TxReply& tx_reply_buf = tx_data->get_reply();
  double last_latency = tx_data->last_attempt_latency();
  tx_data->reply_.res_ = SUCCESS;
  this->Report(tx_reply_buf, last_latency);
  commit_reported_ = true;
}

void CoordinatorClassic::End() {
  TxData* tx_data = (TxData*) cmd_;
  TxReply& tx_reply_buf = tx_data->get_reply();
  double last_latency = tx_data->last_attempt_latency();
  if (committed_) {
    if (!commit_reported_) {
      tx_data->reply_.res_ = SUCCESS;
      this->Report(tx_reply_buf, last_latency
#ifdef TXN_STAT
          , txn
#endif // ifdef TXN_STAT
      );
    }
  } else if (aborted_) {
    tx_data->reply_.res_ = REJECT;
  } else {
    verify(0);
  }
  tx_reply_buf.tx_id_ = ongoing_tx_id_;
  Log_debug("call reply for tx_id: %"
                PRIx64, ongoing_tx_id_);
  tx_data->callback_(tx_reply_buf);
  ongoing_tx_id_ = 0;
  delete tx_data;
}

void CoordinatorClassic::Report(TxReply& txn_reply,
                                double last_latency
#ifdef TXN_STAT
    , TxnChopper *ch
#endif // ifdef TXN_STAT
) {

  bool not_forwarding = forward_status_ != PROCESS_FORWARD_REQUEST;
  if (ccsi_ && not_forwarding) {
    if (txn_reply.res_ == SUCCESS) {
#ifdef TXN_STAT
      txn_stats_[ch->tx_type_].one(ch->proxies_.size(), ch->p_types_);
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

void CoordinatorClassic::___TestPhaseThree(txnid_t txn_id) {
  // auto it = ___phase_three_tids_.find(txn_id);
//  verify(it == ___phase_three_tids_.end());
//  ___phase_three_tids_.insert(txn_id);
}

void CoordinatorClassic::___TestPhaseOne(txnid_t txn_id) {
  auto it = ___phase_one_tids_.find(txn_id);
  verify(it == ___phase_one_tids_.end());
  ___phase_one_tids_.insert(txn_id);
}

} // namespace janus
