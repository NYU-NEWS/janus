

#include "scheduler.h"
#include "commo.h"
#include "../rcc/tx.h"

using namespace janus;

map<txnid_t, shared_ptr<RccTx>> SchedulerJanus::Aggregate(RccGraph &graph) {
  // aggregate vertexes
  map<txnid_t, shared_ptr<RccTx>> index;
  for (auto &pair: graph.vertex_index()) {
    auto rhs_v = pair.second;
    verify(pair.first == rhs_v->id());
    auto vertex = AggregateVertex(rhs_v);
    RccTx &dtxn = *vertex;
    if (dtxn.epoch_ == 0) {
      dtxn.epoch_ = epoch_mgr_.curr_epoch_;
    }
//    epoch_mgr_.AddToEpoch(dtxn.epoch_, dtxn.tid_, dtxn.IsDecided());
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
                                const rank_t rank,
                                const vector<SimpleCommand> &cmds,
                                shared_ptr<RccGraph> graph,
                                shared_ptr<RccGraph> res_graph) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  verify(0);
  return 0;
/**
//  Log_info("on preaccept: %llx par: %d", txn_id, (int)partition_id_);
//  if (RandomGenerator::rand(1, 2000) <= 1)
//    Log_info("on pre-accept graph size: %d", graph.size());
  verify(txn_id > 0);
  verify(cmds[0].root_id_ == txn_id);
  if (graph) {
    Aggregate(*graph);
  }
  // TODO FIXME
  // add interference based on cmds.
  auto dtxn = dynamic_pointer_cast<RccTx>(GetOrCreateTx(txn_id));
  auto& subtx = dtxn->sub_tx_i_;
  subtx.UpdateStatus(TXN_PAC);
  subtx.involve_flag_ = RccTx::RccSubTx::INVOLVED;
  RccTx &tinfo = *dtxn;
  int ret;
  if (subtx.max_seen_ballot_ > 0) {
    ret = REJECT;
  } else {
    if (subtx.status() < TXN_CMT) {
      if (dtxn->phase_ < PHASE_RCC_DISPATCH && subtx.status() < TXN_CMT) {
        for (auto &c: cmds) {
          map<int32_t, Value> output;
          dtxn->DispatchExecute(const_cast<SimpleCommand &>(c), &output);
        }
      }
    } else {
      if (subtx.dreqs_.size() == 0) {
        for (auto &c: cmds) {
          subtx.dreqs_.push_back(c);
        }
      }
    }
    verify(!subtx.fully_dispatched_->value_);
    subtx.fully_dispatched_->Set(1);
    MinItfrGraph(*dtxn, res_graph, false, 1);
    ret = SUCCESS;
  }
  return ret;
  */
}

void SchedulerJanus::OnAccept(const txnid_t txn_id,
                              int rank,
                              const ballot_t &ballot,
                              shared_ptr<RccGraph> graph,
                              int32_t *res) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto dtxn = dynamic_pointer_cast<RccTx>(GetOrCreateTx(txn_id, rank));
  verify(rank = RANK_D);
  auto& subtx = dtxn->subtx(rank);
  if (subtx.max_seen_ballot_ > ballot) {
    *res = REJECT;
    verify(0); // do not support failure recovery so far.
  } else {
    subtx.max_accepted_ballot_ = ballot;
    subtx.max_seen_ballot_ = ballot;
    Aggregate(*graph);
    *res = SUCCESS;
  }
}

//int SchedulerJanus::OnCommit(const txnid_t cmd_id,
//                             rank_t rank,
//                             bool need_validation,
//                             shared_ptr<RccGraph> sp_graph,
//                             TxnOutput *output) {
//  std::lock_guard<std::recursive_mutex> lock(mtx_);
////  if (RandomGenerator::rand(1, 2000) <= 1)
////    Log_info("on commit graph size: %d", graph.size());
//  int ret = SUCCESS;
//  auto dtxn = dynamic_pointer_cast<RccTx>(GetOrCreateTx(cmd_id));
//  dtxn->need_validation_ = need_validation;
//  verify(dtxn->p_output_reply_ == nullptr);
//  dtxn->p_output_reply_ = output;
//  verify(!dtxn->IsAborted());
//  if (dtxn->HasLogApplyStarted()) {
//    ret = SUCCESS; // TODO no return output?
//  } else {
////    Log_info("on commit: %llx par: %d", cmd_id, (int)partition_id_);
////    dtxn->commit_request_received_ = true;
//    if (!sp_graph) {
//      // quick path without graph, no contention.
//      verify(dtxn->fully_dispatched_->value_); //cannot handle non-dispatched now.
//      UpgradeStatus(*dtxn, TXN_DCD);
//      Execute(dtxn);
//    } else {
//      // with graph
//      auto index = Aggregate(*sp_graph);
////      TriggerCheckAfterAggregation(*sp_graph);
//      WaitUntilAllPredecessorsAtLeastCommitting(dtxn.get());
//      RccScc& scc = FindSccPred(*dtxn);
//      Decide(scc);
//      WaitUntilAllPredSccExecuted(scc);
//      if (FullyDispatched(scc) && !scc[0]->HasLogApplyStarted()) {
//        Execute(scc);
//      }
//    }
//    dtxn->sp_ev_commit_->Wait();
//    ret = dtxn->committed_ ? SUCCESS : REJECT;
//  }
//  return ret;
//}

JanusCommo *SchedulerJanus::commo() {

  auto commo = dynamic_cast<JanusCommo *>(commo_);
  verify(commo != nullptr);
  return commo;
}
