#include "marshal-value.h"
#include "coord.h"
#include "frame.h"
#include "dtxn.h"
#include "dep_graph.h"
#include "../benchmark_control_rpc.h"

namespace rococo {


RccCommo* RccCoord::commo() {
  if (commo_ == nullptr) {
    commo_ = frame_->CreateCommo();
    commo_->loc_id_ = loc_id_;
  }
  verify(commo_ != nullptr);
  return dynamic_cast<RccCommo*>(commo_);
}

void RccCoord::PreDispatch() {
  verify(ro_state_ == BEGIN);
  TxnCommand* txn = dynamic_cast<TxnCommand*>(cmd_);
//  auto dispatch = txn->is_read_only() ?
//                  std::bind(&RccCoord::DispatchRo, this) :
//                  std::bind(&RccCoord::Dispatch, this);
  auto dispatch = std::bind(&RccCoord::Dispatch, this);
  if (recorder_) {
    std::string log_s;
    // TODO get appropriate log
//    req.get_log(cmd_->id_, log_s);
    recorder_->submit(log_s, dispatch);
  } else {
    dispatch();
  }
}


void RccCoord::Dispatch() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto txn = (TxnCommand*) cmd_;
  verify(txn->root_id_ == txn->id_);
  int cnt = 0;
  auto cmds_by_par = txn->GetReadyCmds();
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
  verify(txn().root_id_ == txn().id_);
  verify(graph.vertex_index().size() > 0);
  RccDTxn& info = *(graph.vertex_index().at(txn().root_id_));
//  verify(cmd[0].root_id_ == info.id());
//  verify(info.partition_.find(cmd.partition_id_) != info.partition_.end());

  for (auto& pair : output) {
    n_dispatch_ack_++;
    verify(dispatch_acks_[pair.first] == false);
    dispatch_acks_[pair.first] = true;
    txn().Merge(pair.first, pair.second);
    Log_debug("get start ack %ld/%ld for cmd_id: %lx, inn_id: %d",
              n_dispatch_ack_, n_dispatch_, txn().id_, pair.first);
  }

  // where should I store this graph?
  Log_debug("start response graph size: %d", (int)graph.size());
  verify(graph.size() > 0);

  graph_.Aggregate(0, graph);

  // TODO?
  if (graph.size() > 1) txn().disable_early_return();

  if (txn().HasMoreSubCmdReadyNotOut()) {
    Log_debug("command has more sub-cmd, cmd_id: %lx,"
                  " n_started_: %d, n_pieces: %d",
              txn().id_, txn().n_pieces_dispatched_, txn().GetNPieceAll());
    Dispatch();
  } else if (AllDispatchAcked()) {
    Log_debug("receive all start acks, txn_id: %llx; START PREPARE", cmd_->id_);
    verify(!txn().do_early_return());
    GotoNextPhase();
  }
}

/** caller should be thread safe */
void RccCoord::Finish() {
  verify(0);
  TxnCommand *ch = (TxnCommand*) cmd_;
  // commit or abort piece
  Log_debug(
    "send rcc finish requests to %d servers, tid: %llx, graph size: %d",
    (int)ch->partition_ids_.size(),
    cmd_->id_,
    graph_.size());
  RccDTxn* v = graph_.FindV(cmd_->id_);
  RccDTxn& info = *v;
  verify(ch->partition_ids_.size() == info.partition_.size());
  graph_.UpgradeStatus(v, TXN_CMT);

  verify(graph_.size() > 0);

  for (auto& rp : ch->partition_ids_) {
    commo()->SendFinish(rp,
                        cmd_->id_,
                        graph_,
                        std::bind(&RccCoord::FinishAck,
                                  this,
                                  phase_,
                                  SUCCESS,
                                  std::placeholders::_1));
//    Future::safe_release(proxy->async_rcc_finish_txn(req, fuattr));
  }
}

void RccCoord::FinishAck(phase_t phase,
                         int res,
                         map<innid_t, map<int32_t, Value>>& output) {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  verify(phase_ == phase);
  n_finish_ack_++;

  Log_debug("receive finish response. tid: %llx", cmd_->id_);
  txn().outputs_.insert(output.begin(), output.end());

  verify(!txn().do_early_return());
  bool all_acked = (n_finish_ack_ == txn().GetPartitionIds().size());
//  verify(all_acked == txn().OutputReady());
  if (all_acked) {
    // generate a reply and callback.
    Log_debug("deptran callback, %llx", cmd_->id_);
    committed_ = true;
    GotoNextPhase();
  }
}

void RccCoord::DispatchRo() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  int cnt;
  while (cmd_->HasMoreSubCmdReadyNotOut()) {
    auto subcmd = (SimpleCommand*) cmd_->GetNextReadySubCmd();
    subcmd->id_ = next_pie_id();
    verify(subcmd->root_id_ == cmd_->id_);
    n_dispatch_++;
    cnt++;
    Log_debug("send out start request %ld, cmd_id: %lx, inn_id: %d, pie_id: %lx",
              n_dispatch_, cmd_->id_, subcmd->inn_id_, subcmd->id_);
    dispatch_acks_[subcmd->inn_id()] = false;
    commo()->SendHandoutRo(*subcmd,
                           std::bind(&RccCoord::DispatchRoAck,
                                     this,
                                     phase_,
                                     std::placeholders::_1,
                                     std::placeholders::_2,
                                     std::placeholders::_3));
  }
}

void RccCoord::DispatchRoAck(phase_t phase,
                             int res,
                             SimpleCommand &cmd,
                             map<int, mdb::version_t> &vers) {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  verify(phase == phase_);

  auto ch = (TxnCommand*) cmd_;
  cmd_->Merge(cmd);
  curr_vers_.insert(vers.begin(), vers.end());

  Log_debug("receive deptran RO start response, tid: %llx, pid: %llx, ",
             cmd_->id_,
             cmd.inn_id_);

  ch->n_pieces_dispatched_++;
  if (cmd_->HasMoreSubCmdReadyNotOut()) {
    DispatchRo();
  } else if (ch->n_pieces_dispatched_ == ch->GetNPieceAll()) {
    if (last_vers_ == curr_vers_) {
      // TODO
    } else {
      ch->read_only_reset();
      last_vers_ = curr_vers_;
      curr_vers_.clear();
      this->Dispatch();
    }
  }
}

void RccCoord::Reset() {
  ClassicCoord::Reset();
  graph_.Clear();
  verify(graph_.size() == 0);
  verify(graph_.vertex_index().size() == 0);
  ro_state_ = BEGIN;
  last_vers_.clear();
  curr_vers_.clear();
}


void RccCoord::GotoNextPhase() {
  int n_phase = 3;
  int current_phase = phase_ % n_phase;
  switch (phase_++ % n_phase) {
    case Phase::INIT_END:
      PreDispatch();
      verify(phase_ % n_phase == Phase::DISPATCH);
      break;
    case Phase::DISPATCH:
      verify(phase_ % n_phase == Phase::COMMIT);
      Finish();
      break;
    case Phase::COMMIT:
      verify(phase_ % n_phase == Phase::INIT_END);
      End();
      break;
    default:
      verify(0);
  }
}

} // namespace rococo
