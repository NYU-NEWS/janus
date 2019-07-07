

#include "scheduler.h"
#include "commo.h"
#include "deptran/rococo/tx.h"

using namespace janus;

map<txnid_t, shared_ptr<TxRococo>> SchedulerJanus::Aggregate(RccGraph &graph) {
  // aggregate vertexes
  map<txnid_t, shared_ptr<TxRococo>> index;
  for (auto &pair: graph.vertex_index()) {
    auto rhs_v = pair.second;
    verify(pair.first == rhs_v->id());
    auto vertex = AggregateVertex(rhs_v);
    TxRococo &dtxn = *vertex;
    if (dtxn.epoch_ == 0) {
      dtxn.epoch_ = epoch_mgr_.curr_epoch_;
    }
    epoch_mgr_.AddToEpoch(dtxn.epoch_, dtxn.tid_, dtxn.IsDecided());
    verify(vertex->id() == pair.second->id());
    verify(vertex_index().count(vertex->id()) > 0);
    index[vertex->id()] = vertex;
  }
  // aggregate edges.
  RebuildEdgePointer(index);

#ifdef DEBUG_CODE
  verify(index.size() == graph.vertex_index_.size());
  for (auto& pair: index) {
    txnid_t txnid = pair.first;
    RccVertex* v = pair.second;
    verify(v->parents_.size() == v->incoming_.size());
    auto sz = v->parents_.size();
    if (v->Get().status() >= TXN_CMT)
      RccSched::__DebugCheckParentSetSize(txnid, sz);
  }

  for (auto& pair: graph.vertex_index_) {
    auto txnid = pair.first;
    RccVertex* rhs_v = pair.second;
    auto lhs_v = FindV(txnid);
    verify(lhs_v != nullptr);
    // TODO, check the Sccs are the same.
    if (rhs_v->Get().status() >= TXN_DCD) {
      verify(lhs_v->Get().status() >= TXN_DCD);
      if (!AllAncCmt(rhs_v))
        continue;
      RccScc& rhs_scc = graph.FindSCC(rhs_v);
      for (RccVertex* rhs_vv : rhs_scc) {
        verify(rhs_vv->Get().status() >= TXN_DCD);
        RccVertex* lhs_vv = FindV(rhs_vv->id());
        verify(lhs_vv != nullptr);
        verify(lhs_vv->Get().status() >= TXN_DCD);
        verify(lhs_vv->GetParentSet() == rhs_vv->GetParentSet());
      }
      if (!AllAncCmt(lhs_v)) {
        continue;
      }
      RccScc& lhs_scc = FindSCC(lhs_v);
      for (RccVertex* lhs_vv : rhs_scc) {
        verify(lhs_vv->Get().status() >= TXN_DCD);
        RccVertex* rhs_vv = graph.FindV(lhs_v->id());
        verify(rhs_vv != nullptr);
        verify(rhs_vv->Get().status() >= TXN_DCD);
        verify(rhs_vv->GetParentSet() == rhs_vv->GetParentSet());
      }

      auto lhs_sz = lhs_scc.size();
      auto rhs_sz = rhs_scc.size();
      if (lhs_sz != rhs_sz) {
        // TODO
        for (auto& vv : rhs_scc) {
          auto vvv = FindV(vv->id());
          verify(vvv->Get().status() >= TXN_DCD);
        }
        verify(0);
      }
//      verify(sz >= rhs_sz);
//      verify(sz == rhs_sz);
      for (auto vv : rhs_scc) {
        bool r = std::any_of(lhs_scc.begin(),
                             lhs_scc.end(),
                             [vv] (RccVertex* vvv) -> bool {
                               return vvv->id() == vv->id();
                             }
        );
        verify(r);
      }
    }

    if (lhs_v->Get().status() >= TXN_DCD && AllAncCmt(lhs_v)) {
      RccScc& scc = FindSCC(lhs_v);
      for (auto vv : scc) {
        auto s = vv->Get().status();
        verify(s >= TXN_DCD);
      }
    }
  }

#endif
//  this->BuildEdgePointer(graph, index);
  return index;
}

int SchedulerJanus::OnPreAccept(const txid_t txn_id,
                                const vector<SimpleCommand> &cmds,
                                shared_ptr<RccGraph> graph,
                                shared_ptr<RccGraph> res_graph) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
//  Log_info("on preaccept: %llx par: %d", txn_id, (int)partition_id_);
//  if (RandomGenerator::rand(1, 2000) <= 1)
//    Log_info("on pre-accept graph size: %d", graph.size());
  verify(txn_id > 0);
  verify(cmds[0].root_id_ == txn_id);
  if (graph) {
    Aggregate(*graph);
    TriggerCheckAfterAggregation(*graph);
  }
  // TODO FIXME
  // add interference based on cmds.
  auto dtxn = dynamic_pointer_cast<TxRococo>(GetOrCreateTx(txn_id));
  dtxn->UpdateStatus(TXN_PAC);
  dtxn->involve_flag_ = TxRococo::INVOLVED;
  TxRococo &tinfo = *dtxn;
  int ret;
  if (dtxn->max_seen_ballot_ > 0) {
    ret = REJECT;
  } else {
    if (dtxn->status() < TXN_CMT) {
      if (dtxn->phase_ < PHASE_RCC_DISPATCH && tinfo.status() < TXN_CMT) {
        for (auto &c: cmds) {
          map<int32_t, Value> output;
          dtxn->DispatchExecute(const_cast<SimpleCommand &>(c), &output);
        }
      }
    } else {
      if (dtxn->dreqs_.size() == 0) {
        for (auto &c: cmds) {
          dtxn->dreqs_.push_back(c);
        }
      }
    }
    verify(!tinfo.fully_dispatched);
    tinfo.fully_dispatched = true;
    MinItfrGraph(*dtxn, res_graph, false, 1);
    if (tinfo.status() >= TXN_CMT) {
      waitlist_.insert(dtxn.get());
      verify(dtxn->epoch_ > 0);
    }
    ret = SUCCESS;
  }
  return ret;
}

void SchedulerJanus::OnAccept(const txnid_t txn_id,
                              const ballot_t &ballot,
                              shared_ptr<RccGraph> graph,
                              int32_t *res) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto dtxn = dynamic_pointer_cast<TxRococo>(GetOrCreateTx(txn_id));
  if (dtxn->max_seen_ballot_ > ballot) {
    *res = REJECT;
    verify(0); // do not support failure recovery so far.
  } else {
    dtxn->max_accepted_ballot_ = ballot;
    dtxn->max_seen_ballot_ = ballot;
    Aggregate(*graph);
    *res = SUCCESS;
  }
}
//
//void SchedulerJanus::OnCommit(const txnid_t cmd_id,
//                        const RccGraph& graph,
//                        int32_t* res,
//                        TxnOutput* output,
//                        const function<void()>& callback) {
//  // TODO to support cascade abort
//  std::lock_guard<std::recursive_mutex> lock(mtx_);
////  if (RandomGenerator::rand(1, 2000) <= 1)
////    Log_info("on commit graph size: %d", graph.size());
//  *res = SUCCESS;
//  // union the graph into dep graph
//  RccDTxn *dtxn = (RccDTxn*) GetOrCreateDTxn(cmd_id);
//  verify(dtxn != nullptr);
//  RccDTxn& info = *dtxn;
//
//  verify(dtxn->ptr_output_repy_ == nullptr);
//  dtxn->ptr_output_repy_ = output;
//
//  if (info.IsExecuted()) {
//    verify(info.status() >= TXN_DCD);
//    verify(info.graphs_for_inquire_.size() == 0);
//    *res = SUCCESS;
//    callback();
//  } else if (info.IsAborted()) {
//    verify(0);
//    *res = REJECT;
//    callback();
//  } else {
////    Log_info("on commit: %llx par: %d", cmd_id, (int)partition_id_);
//    dtxn->commit_request_received_ = true;
//    dtxn->finish_reply_callback_ = [callback, res] (int r) {
//      *res = r;
////      verify(r == SUCCESS);
//      callback();
//    };
//    auto index = Aggregate(const_cast<RccGraph&> (graph));
//    for (auto& pair: index) {
//      verify(pair.second->epoch_ > 0);
//    }
//    TriggerCheckAfterAggregation(const_cast<RccGraph &>(graph));
//    // fast path without check wait list?
////    if (graph.size() == 1) {
////      auto v = dep_graph_->FindV(cmd_id);
////      if (v->incoming_.size() == 0);
////      CheckInquired(v->Get());
////      Execute(v->Get());
////      return;
////    } else {
////      Log_debug("graph size on commit, %d", (int) graph.size());
//////    verify(0);
////    }
//  }
//
//}

//
//void SchedulerJanus::OnCommitWoGraph(const txnid_t cmd_id,
//                               int32_t* res,
//                               TxnOutput* output,
//                               const function<void()>& callback) {
//  // TODO to support cascade abort
//  std::lock_guard<std::recursive_mutex> lock(mtx_);
//  *res = SUCCESS;
//  // union the graph into dep graph
//  RccDTxn *dtxn = (RccDTxn*) GetOrCreateDTxn(cmd_id);
//  verify(dtxn != nullptr);
//  RccDTxn& info = *dtxn;
//
//  verify(dtxn->ptr_output_repy_ == nullptr);
//  dtxn->ptr_output_repy_ = output;
//
//  if (info.IsExecuted()) {
//    verify(info.status() >= TXN_DCD);
//    verify(info.graphs_for_inquire_.size() == 0);
//    *res = SUCCESS;
//    callback();
//  } else if (info.IsAborted()) {
//    verify(0);
//    *res = REJECT;
//    callback();
//  } else {
////    Log_info("on commit: %llx par: %d", cmd_id, (int)partition_id_);
//    dtxn->commit_request_received_ = true;
//    dtxn->finish_reply_callback_ = [callback, res] (int r) {
//      *res = r;
////      verify(r == SUCCESS);
//      callback();
//    };
//    UpgradeStatus(dtxn, TXN_CMT);
//    waitlist_.insert(dtxn);
//    verify(dtxn->epoch_ > 0);
//    CheckWaitlist();
//  }
//}



int SchedulerJanus::OnInquire(epoch_t epoch,
                              cmdid_t cmd_id,
                              shared_ptr<RccGraph> graph) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  // TODO check epoch, cannot be a too old one.
  auto sp_tx = dynamic_pointer_cast<TxRococo>(GetOrCreateTx(cmd_id));
  //register an event, triggered when the status >= COMMITTING;
  verify (sp_tx->Involve(Scheduler::partition_id_));

  // TODO improve this with a more advanced event.
  if (sp_tx->status() < TXN_CMT) {
    waitlist_.insert(sp_tx.get());
  }
  sp_tx->status_.Wait([](int v)->bool {return v>=TXN_CMT;});
  InquiredGraph(*sp_tx, graph);
  return 0;
}

int SchedulerJanus::OnCommit(const txnid_t cmd_id,
                             shared_ptr<RccGraph> sp_graph,
                             TxnOutput *output) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
//  if (RandomGenerator::rand(1, 2000) <= 1)
//    Log_info("on commit graph size: %d", graph.size());
  int ret = SUCCESS;
  // union the graph into dep graph
  auto dtxn = dynamic_pointer_cast<TxRococo>(GetOrCreateTx(cmd_id));
  verify(dtxn->ptr_output_repy_ == nullptr);
  dtxn->ptr_output_repy_ = output;

  if (dtxn->IsExecuted()) {
//    verify(info.status() >= TXN_DCD);
//    verify(info.graphs_for_inquire_.size() == 0);
    ret = SUCCESS;
  } else if (dtxn->IsAborted()) {
    verify(0);
    ret = REJECT;
  } else {
//    Log_info("on commit: %llx par: %d", cmd_id, (int)partition_id_);
    dtxn->commit_request_received_ = true;
    if (!sp_graph) {
      // quick path without graph, no contention.
      verify(dtxn->fully_dispatched); //cannot handle non-dispatched now.
      UpgradeStatus(*dtxn, TXN_DCD);
      Execute(*dtxn);
      if (dtxn->to_checks_.size() > 0) {
        for (auto child : dtxn->to_checks_) {
          waitlist_.insert(child);
        }
        CheckWaitlist();
      }
    } else {
      // with graph
      auto index = Aggregate(*sp_graph);
//      for (auto& pair: index) {
//        verify(pair.second->epoch_ > 0);
//      }
      TriggerCheckAfterAggregation(*sp_graph);
    }
    dtxn->sp_ev_commit_->Wait();
    ret = dtxn->committed_ ? SUCCESS : REJECT;
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
  return ret;
}

JanusCommo *SchedulerJanus::commo() {

  auto commo = dynamic_cast<JanusCommo *>(commo_);
  verify(commo != nullptr);
  return commo;
}
