

#include "sched.h"
#include "commo.h"
#include "../rcc/dtxn.h"

using namespace rococo;

void BrqSched::OnPreAccept(const txnid_t txn_id,
                           const vector<SimpleCommand>& cmds,
                           const RccGraph& graph,
                           int32_t* res,
                           RccGraph* res_graph,
                           function<void()> callback) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
//  Log_info("on preaccept: %llx par: %d", txn_id, (int)partition_id_);
//  if (RandomGenerator::rand(1, 2000) <= 1)
//    Log_info("on pre-accept graph size: %d", graph.size());
  verify(txn_id > 0);
  verify(cmds[0].root_id_ == txn_id);
  dep_graph_->Aggregate(const_cast<RccGraph&>(graph));
  TriggerCheckAfterAggregation(const_cast<RccGraph&>(graph));
  // TODO FIXME
  // add interference based on cmds.
  RccDTxn *dtxn = (RccDTxn *) GetOrCreateDTxn(txn_id);
//  TxnInfo& tinfo = dtxn->tv_->Get();
  if (dtxn->status() < TXN_CMT) {
    if (dtxn->phase_ < PHASE_RCC_DISPATCH && dtxn->status() < TXN_CMT) {
      for (auto& c: cmds) {
        map<int32_t, Value> output;
        dtxn->DispatchExecute(c, res, &output);
      }
    }
  } else {
    if (dtxn->dreqs_.size() == 0) {
      for (auto& c: cmds) {
        dtxn->dreqs_.push_back(c);
      }
    }
  }
  verify(!dtxn->fully_dispatched);
  dtxn->fully_dispatched = true;
  dep_graph_->MinItfrGraph(txn_id, res_graph, false, 1);
  if (dtxn->status() >= TXN_CMT) {
    waitlist_.insert(dtxn->tv_);
  }
  *res = SUCCESS;
  callback();
  TriggerUpgradeEpoch();
}

void BrqSched::OnPreAcceptWoGraph(const txnid_t txn_id,
                                  const vector<SimpleCommand>& cmds,
                                  int32_t* res,
                                  RccGraph* res_graph,
                                  function<void()> callback) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
//  Log_info("on preaccept: %llx par: %d", txn_id, (int)partition_id_);
//  if (RandomGenerator::rand(1, 2000) <= 1)
//    Log_info("on pre-accept graph size: %d", graph.size());
  verify(txn_id > 0);
  verify(cmds[0].root_id_ == txn_id);
//  dep_graph_->Aggregate(const_cast<RccGraph&>(graph));
//  TriggerCheckAfterAggregation(const_cast<RccGraph&>(graph));
  // TODO FIXME
  // add interference based on cmds.
  RccDTxn *dtxn = (RccDTxn *) GetOrCreateDTxn(txn_id);
  RccDTxn& tinfo = *dtxn;
  if (tinfo.status() < TXN_CMT) {
    if (dtxn->phase_ < PHASE_RCC_DISPATCH && tinfo.status() < TXN_CMT) {
      for (auto& c: cmds) {
        map<int32_t, Value> output;
        dtxn->DispatchExecute(c, res, &output);
      }
    }
  } else {
    if (dtxn->dreqs_.size() == 0) {
      for (auto& c: cmds) {
        dtxn->dreqs_.push_back(c);
      }
    }
  }
  verify(!tinfo.fully_dispatched);
  tinfo.fully_dispatched = true;
  dep_graph_->MinItfrGraph(txn_id, res_graph, false, 1);
  if (tinfo.status() >= TXN_CMT) {
    waitlist_.insert(dtxn->tv_);
  }
  *res = SUCCESS;
  callback();
}


void BrqSched::OnAccept(const txnid_t txn_id,
                        const ballot_t& ballot,
                        const RccGraph& graph,
                        int32_t* res,
                        function<void()> callback) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  // TODO
  *res = SUCCESS;
  callback();
}

void BrqSched::OnCommit(const txnid_t cmd_id,
                        const RccGraph& graph,
                        int32_t* res,
                        TxnOutput* output,
                        const function<void()>& callback) {
  // TODO to support cascade abort
  std::lock_guard<std::recursive_mutex> lock(mtx_);
//  if (RandomGenerator::rand(1, 2000) <= 1)
//    Log_info("on commit graph size: %d", graph.size());
  *res = SUCCESS;
  // union the graph into dep graph
  RccDTxn *dtxn = (RccDTxn*) GetOrCreateDTxn(cmd_id);
  verify(dtxn != nullptr);
  verify(dtxn->tv_ != nullptr);
  auto v = dtxn->tv_;
  RccDTxn& info = v->Get();

  verify(dtxn->ptr_output_repy_ == nullptr);
  dtxn->ptr_output_repy_ = output;

  if (info.IsExecuted()) {
    verify(info.status() >= TXN_DCD);
    verify(info.graphs_for_inquire_.size() == 0);
    *res = SUCCESS;
    callback();
  } else if (info.IsAborted()) {
    verify(0);
    *res = REJECT;
    callback();
  } else {
//    Log_info("on commit: %llx par: %d", cmd_id, (int)partition_id_);
    dtxn->commit_request_received_ = true;
    dtxn->finish_reply_callback_ = [callback, res] (int r) {
      *res = r;
//      verify(r == SUCCESS);
      callback();
    };
    dep_graph_->Aggregate(const_cast<RccGraph&>(graph));
    TriggerCheckAfterAggregation(const_cast<RccGraph &>(graph));
    // fast path without check wait list?
//    if (graph.size() == 1) {
//      auto v = dep_graph_->FindV(cmd_id);
//      if (v->incoming_.size() == 0);
//      CheckInquired(v->Get());
//      Execute(v->Get());
//      return;
//    } else {
//      Log_debug("graph size on commit, %d", (int) graph.size());
////    verify(0);
//    }
  }

}


void BrqSched::OnCommitWoGraph(const txnid_t cmd_id,
                               int32_t* res,
                               TxnOutput* output,
                               const function<void()>& callback) {
  // TODO to support cascade abort
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  *res = SUCCESS;
  // union the graph into dep graph
  RccDTxn *dtxn = (RccDTxn*) GetOrCreateDTxn(cmd_id);
  verify(dtxn != nullptr);
  verify(dtxn->tv_ != nullptr);
  auto v = dtxn->tv_;
  RccDTxn& info = v->Get();

  verify(dtxn->ptr_output_repy_ == nullptr);
  dtxn->ptr_output_repy_ = output;

  if (info.IsExecuted()) {
    verify(info.status() >= TXN_DCD);
    verify(info.graphs_for_inquire_.size() == 0);
    *res = SUCCESS;
    callback();
  } else if (info.IsAborted()) {
    verify(0);
    *res = REJECT;
    callback();
  } else {
//    Log_info("on commit: %llx par: %d", cmd_id, (int)partition_id_);
    dtxn->commit_request_received_ = true;
    dtxn->finish_reply_callback_ = [callback, res] (int r) {
      *res = r;
//      verify(r == SUCCESS);
      callback();
    };
    dep_graph_->UpgradeStatus(v, TXN_CMT);
    waitlist_.insert(v);
    CheckWaitlist();
    // fast path without check wait list?
//    if (graph.size() == 1) {
//      auto v = dep_graph_->FindV(cmd_id);
//      if (v->incoming_.size() == 0);
//      CheckInquired(v->Get());
//      Execute(v->Get());
//      return;
//    } else {
//      Log_debug("graph size on commit, %d", (int) graph.size());
////    verify(0);
//    }
  }

}


int BrqSched::OnInquire(cmdid_t cmd_id,
                        RccGraph *graph,
                        const function<void()> &callback) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  RccDTxn *dtxn = (RccDTxn *) GetOrCreateDTxn(cmd_id);
  RccVertex* v = dtxn->tv_;
  verify(v != nullptr);
  RccDTxn& info = v->Get();
  //register an event, triggered when the status >= COMMITTING;
  verify (info.Involve(partition_id_));

  auto cb_wrapper = [callback, graph] () {
#ifdef DEBUG_CODE
    for (auto pair : graph->vertex_index_) {
      RccVertex* v = pair.second;
      TxnInfo& tinfo = v->Get();
      if (tinfo.status() >= TXN_CMT) {
//        Log_info("inquire ack, txnid: %llx, parent size: %d",
//                 pair.first, v->GetParentSet().size());
        RccSched::__DebugCheckParentSetSize(v->id(), v->parents_.size());
      }
    }
#endif
    callback();
  };

  if (info.status() >= TXN_CMT) {
    dep_graph_->MinItfrGraph(cmd_id, graph, false, 1);
    cb_wrapper();
  } else {
    info.graphs_for_inquire_.push_back(graph);
    info.callbacks_for_inquire_.push_back(cb_wrapper);
    verify(info.graphs_for_inquire_.size() ==
        info.callbacks_for_inquire_.size());
    waitlist_.insert(v);
  }

}

BrqCommo* BrqSched::commo() {

  auto commo = dynamic_cast<BrqCommo*>(commo_);
  verify(commo != nullptr);
  return commo;
}

void BrqSched::TriggerUpgradeEpoch() {
  if (site_id_ == 0) {
    auto t_now = std::time(nullptr);
    auto d = std::difftime(t_now, last_upgrade_time_);
    if (d < EPOCH_DURATION || in_upgrade_epoch_) {
      return;
    }
    last_upgrade_time_ = t_now;
    in_upgrade_epoch_ = true;
    commo()->SendUpgradeEpoch(curr_epoch_, std::bind(&BrqSched::UpgradeEpochAck,
                                                     this,
                                                     std::placeholders::_1,
                                                     std::placeholders::_2,
                                                     std::placeholders::_3));
  }
}

void BrqSched::UpgradeEpochAck(parid_t par_id,
                               siteid_t site_id,
                               int32_t res) {
  auto parids = Config::GetConfig()->GetAllPartitionIds();
  for (auto p : parids) {
    if (epoch_replies_.count(p) == 0) {
      epoch_replies_[p] = 0;
    }
  }
  verify(res == 0);
  epoch_replies_[par_id] ++;
  for (auto& pair: epoch_replies_) {
    auto par_id = pair.first;
    auto par_size = Config::GetConfig()->GetPartitionSize(par_id);
    verify(epoch_replies_[par_id] <= par_size);
    if (epoch_replies_[par_id] != par_size) {
      return;
    }
  }

  in_upgrade_epoch_ = false;
  epoch_replies_.clear();
  TriggerTruncateEpoch();
}

void BrqSched::TriggerTruncateEpoch() {
  if (curr_epoch_ > 2) {
    commo()->SendTruncateEpoch(curr_epoch_ - 2);
  }
}