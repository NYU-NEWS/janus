#include "marshal-value.h"
#include "coord.h"
#include "frame.h"
#include "tx.h"
#include "dep_graph.h"
#include "../benchmark_control_rpc.h"

namespace janus {


RccCommo* RccCoord::commo() {
  if (commo_ == nullptr) {
    commo_ = frame_->CreateCommo(nullptr);
    commo_->loc_id_ = loc_id_;
  }
  verify(commo_ != nullptr);
  return dynamic_cast<RccCommo*>(commo_);
}

void RccCoord::PreDispatch() {
  verify(ro_state_ == BEGIN);
  auto dispatch = std::bind(&RccCoord::DispatchAsync, this);
  if (recorder_) {
    std::string log_s;
    verify(0);
    // TODO get appropriate log
//    req.get_log(cmd_->id_, log_s);
    //recorder_->submit(log_s, dispatch);
  } else {
    dispatch();
  }
}


void RccCoord::DispatchAsync() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto txn = (TxData*) cmd_;
  verify(txn->root_id_ == txn->id_);
  int cnt = 0;
  auto cmds_by_par = txn->GetReadyPiecesData();
  for (auto& pair: cmds_by_par) {
    const parid_t& par_id = pair.first;
    auto& cmds = pair.second;
    n_dispatch_ += cmds.size();
    cnt += cmds.size();
    vector<SimpleCommand> cc;
    for (auto c: cmds) {
      if (mocking_janus_) {
        c->rank_ = RANK_D;
      }
      if (c->rank_ == RANK_I) {
        par_i_.insert(c->PartitionId());
      } else {
        par_d_.insert(c->PartitionId());
      }
      if (c->rank_ == RANK_UNDEFINED) {
        c->rank_ = RANK_D;
      }
      c->id_ = next_pie_id();
      dispatch_acks_[c->inn_id_] = false;
      cc.push_back(*c);
    }
    commo()->SendDispatch(cc,
                          std::bind(&RccCoord::DispatchAck,
                                    this,
                                    phase_,
                                    std::placeholders::_1,
                                    std::placeholders::_2,
                                    std::placeholders::_3));
  }
}

void RccCoord::DispatchAck(phase_t phase,
                           int res,
                           TxnOutput& output,
                           RccGraph &graph) {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  verify(phase == phase_); // cannot proceed without all acks.
  verify(tx_data().root_id_ == tx_data().id_);
//  verify(graph.vertex_index().size() > 0);
//  RccTx& info = *(graph.vertex_index().at(tx_data().root_id_));
//  verify(cmd[0].root_id_ == info.id());
//  verify(info.partition_.find(cmd.partition_id_) != info.partition_.end());
  if (res) {
    // need validation
    tx_data().need_validation_ = true;
  }

  for (auto& pair : output) {
    n_dispatch_ack_++;
    verify(dispatch_acks_[pair.first] == false);
    dispatch_acks_[pair.first] = true;
    tx_data().Merge(pair.first, pair.second);
    Log_debug("get start ack %ld/%ld for cmd_id: %lx, inn_id: %d",
              n_dispatch_ack_, n_dispatch_, tx_data().id_, pair.first);
  }

  // where should I store this graph?
//  Log_debug("start response graph size: %d", (int)graph.size());
//  verify(graph.size() > 0);

//  sp_graph_->Aggregate(0, graph);

  // TODO?
//  if (graph.size() > 1) tx_data().disable_early_return();

  if (tx_data().HasMoreUnsentPiece()) {
    Log_debug("command has more sub-cmd, cmd_id: %lx,"
                  " n_started_: %d, n_pieces: %d",
              tx_data().id_,
              tx_data().n_pieces_dispatched_, tx_data().GetNPieceAll());
    DispatchAsync();
  } else if (AllDispatchAcked()) {
    Log_debug("receive all start acks, txn_id: %llx; START PREPARE", cmd_->id_);
    verify(!tx_data().do_early_return());
    GotoNextPhase();
  }
}

/** caller should be thread safe */
void RccCoord::Finish() {
  verify(0);
//  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
//  TxData *ch = (TxData*) cmd_;
//  // commit or abort piece
//  Log_debug(
//    "send rococo finish requests to %d servers, tid: %llx, graph size: %d",
//    (int)ch->partition_ids_.size(), cmd_->id_, sp_graph_->size());
//  auto v = sp_graph_->FindV(cmd_->id_);
//  RccTx& info = *v;
//  verify(ch->partition_ids_.size() == info.partition_.size());
//  sp_graph_->UpgradeStatus(*v, TXN_CMT);
//  verify(sp_graph_->size() > 0);
//
//  for (auto& rp : ch->partition_ids_) {
//    commo()->SendFinish(rp,
//                        cmd_->id_,
//                        sp_graph_,
//                        std::bind(&RccCoord::FinishAck,
//                                  this,
//                                  phase_,
//                                  SUCCESS,
//                                  std::placeholders::_1));
//  }
}

void RccCoord::FinishAck(phase_t phase,
                         int res,
                         map<innid_t, map<int32_t, Value>>& output) {
  verify(0);
//  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
//  verify(phase_ == phase);
//  n_finish_ack_++;
//
//  Log_debug("receive finish response. tid: %llx", cmd_->id_);
//  tx_data().outputs_.insert(output.begin(), output.end());
//
//  verify(!tx_data().do_early_return());
//  bool all_acked = (n_finish_ack_ == tx_data().GetPartitionIds().size());
////  verify(all_acked == txn().OutputReady());
//  if (all_acked) {
//    // generate a reply and callback.
//    Log_debug("deptran callback, %llx", cmd_->id_);
//    committed_ = true;
//    GotoNextPhase();
//  }
}


void RccCoord::Commit() {
  verify(0);
//  std::lock_guard<std::recursive_mutex> guard(mtx_);
//  TxData* txn = (TxData*) cmd_;
//  auto dtxn = sp_graph_->FindV(cmd_->id_);
//  verify(txn->partition_ids_.size() == dtxn->partition_.size());
//  sp_graph_->UpgradeStatus(*dtxn, TXN_CMT);
//  for (auto par_id : cmd_->GetPartitionIds()) {
//    commo()->BroadcastCommit(par_id,
//                             cmd_->id_,
//                             RANK_UNDEFINED,
//                             txn->need_validation_,
//                             sp_graph_,
//                             std::bind(&RccCoord::CommitAck,
//                                       this,
//                                       phase_,
//                                       par_id,
//                                       std::placeholders::_1,
//                                       std::placeholders::_2));
//  }
//  if (fast_commit_) {
//    committed_ = true;
//    GotoNextPhase();
//  }
}

void RccCoord::CommitAck(phase_t phase,
                                 parid_t par_id,
                                 int32_t res,
                                 TxnOutput& output) {
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  if (phase != phase_) return;
  if (fast_commit_) return;
  if (res == SUCCESS) {
    committed_ = true;
  } else if (res == REJECT) {
    aborted_ = true;
  } else {
    verify(0);
  }
  n_commit_oks_[par_id]++;
  if (n_commit_oks_[par_id] > 1)
    return; // TODO debug

//  txn().Merge(output);
  // if collect enough results.
  // if there are still more results to collect.
  GotoNextPhase();
//  bool all_acked = txn().OutputReady();
//  if (all_acked)
//  GotoNextPhase();
  return;
}



void RccCoord::DispatchRo() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  int cnt;
  verify(0); // TODO
//  while (cmd_->HasMoreSubCmdReadyNotOut()) {
//    auto subcmd = (SimpleCommand*) cmd_->GetNextReadySubCmd();
//    subcmd->id_ = next_pie_id();
//    verify(subcmd->root_id_ == cmd_->id_);
//    n_dispatch_++;
//    cnt++;
//    Log_debug("send out start request %ld, cmd_id: %lx, inn_id: %d, pie_id: %lx",
//              n_dispatch_, cmd_->id_, subcmd->inn_id_, subcmd->id_);
//    dispatch_acks_[subcmd->inn_id()] = false;
//    commo()->SendHandoutRo(*subcmd,
//                           std::bind(&RccCoord::DispatchRoAck,
//                                     this,
//                                     phase_,
//                                     std::placeholders::_1,
//                                     std::placeholders::_2,
//                                     std::placeholders::_3));
//  }
}

void RccCoord::DispatchRoAck(phase_t phase,
                             int res,
                             SimpleCommand &cmd,
                             map<int, mdb::version_t> &vers) {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  verify(phase == phase_);

  auto ch = (TxData*) cmd_;
  cmd_->Merge(cmd);
  curr_vers_.insert(vers.begin(), vers.end());

  Log_debug("receive deptran RO start response, tid: %llx, pid: %llx, ",
             cmd_->id_,
             cmd.inn_id_);

  ch->n_pieces_dispatched_++;
  verify(0); // TODO
//  if (cmd_->HasMoreSubCmdReadyNotOut()) {
//    DispatchRo();
//  } else if (ch->n_pieces_dispatched_ == ch->GetNPieceAll()) {
//    if (last_vers_ == curr_vers_) {
//      // TODO
//    } else {
//      ch->read_only_reset();
//      last_vers_ = curr_vers_;
//      curr_vers_.clear();
//      this->DispatchAsync();
//    }
//  }
}

void RccCoord::Reset() {
  CoordinatorClassic::Reset();
//  sp_graph_->Clear();
//  verify(sp_graph_->size() == 0);
//  verify(sp_graph_->vertex_index().size() == 0);
  ro_state_ = BEGIN;
  parents_.clear();
  last_vers_.clear();
  curr_vers_.clear();
  n_commit_oks_.clear();
  fast_commit_ = false;
  rank_ = RANK_UNDEFINED;
  par_i_.clear();
  par_d_.clear();
}


void RccCoord::GotoNextPhase() {
  int n_phase = 3;
  int current_phase = phase_ % n_phase;
  phase_++;
  switch (current_phase % n_phase) {
    case Phase::INIT_END:
      PreDispatch();
      verify(phase_ % n_phase == Phase::DISPATCH);
      break;
    case Phase::DISPATCH:
      verify(phase_ % n_phase == Phase::COMMIT);
      Commit();
      break;
    case Phase::COMMIT:
      verify(phase_ % n_phase == Phase::INIT_END);
      End();
      break;
    default:
      verify(0);
  }
}

} // namespace janus
