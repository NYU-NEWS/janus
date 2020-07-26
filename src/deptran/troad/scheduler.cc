

#include "troad.h"
#include "tx.h"
#include "commo.h"

using namespace janus;

map<txnid_t, shared_ptr<RccTx>> SchedulerTroad::Aggregate(RccGraph &graph) {
  // aggregate vertexes
  map<txnid_t, shared_ptr<RccTx>> index;
  for (auto &pair: graph.vertex_index()) {
    auto rhs_v = pair.second;
    auto vertex = AggregateVertex(rhs_v);
    verify(pair.first == rhs_v->id());
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

int SchedulerTroad::OnPreAccept(const txid_t txn_id,
                                const rank_t rank,
                                const vector<SimpleCommand> &cmds,
                                shared_ptr<RccGraph> graph,
                                shared_ptr<RccGraph> res_graph) {
  verify(0);
  return 0;
/**
  std::lock_guard<std::recursive_mutex> lock(mtx_);
//  Log_info("on preaccept: %llx par: %d", txn_id, (int)partition_id_);
//  if (RandomGenerator::rand(1, 2000) <= 1)
//    Log_info("on pre-accept graph size: %d", graph.size());
  verify(txn_id > 0);
  verify(cmds[0].root_id_ == txn_id);
  verify(rank == RANK_I || rank == RANK_D);
  if (graph) {
//    verify(graph->size() == 0);
    Aggregate(*graph);
  }
  auto dtxn = dynamic_pointer_cast<TxTroad>(GetOrCreateTx(txn_id));
  verify(dtxn->current_rank_ == rank);
  dtxn->UpdateStatus(TXN_PAC);
  dtxn->involve_flag_ = RccTx::INVOLVED;
  if (dtxn->max_seen_ballot_ > 0) {
    return REJECT;
  }
  // assuming an ordered message queue and inquire depth always as 1.
//  verify(dtxn->status() < TXN_CMT); TODO re-enable this check.
  if (dtxn->status() < TXN_CMT) {
    if (dtxn->phase_ <= PHASE_RCC_DISPATCH) {
      for (auto &c: cmds) {
        map<int32_t, Value> output;
        dtxn->DispatchExecute(const_cast<SimpleCommand &>(c), &output); // track dependencies
      }
    }
  } else {
    if (dtxn->dreqs_.size() == 0) {
      for (auto &c: cmds) {
        dtxn->dreqs_.push_back(c);
      }
    }
  }
  verify(!dtxn->fully_dispatched_->value_);
  dtxn->fully_dispatched_->Set(1);
  MinItfrGraph(*dtxn, res_graph, false, 1);
  return SUCCESS;
*/
}

int SchedulerTroad::OnAccept(const txnid_t txn_id,
                             const rank_t rank,
                             const ballot_t &ballot,
                             shared_ptr<RccGraph> graph) {
  verify(0);
  /*
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto dtxn = dynamic_pointer_cast<TxTroad>(GetOrCreateTx(txn_id));
  verify(dtxn->current_rank_ == rank);
  if (dtxn->max_seen_ballot_ > ballot) {
    verify(0); // do not support failure recovery at the moment.
    return REJECT;
  } else {
    Aggregate(*graph);
    dtxn->max_seen_ballot_ = ballot;
    dtxn->max_accepted_ballot_ = ballot;
    return SUCCESS;
  }
   */
}

int SchedulerTroad::OnCommit(const txnid_t cmd_id,
                             const rank_t rank,
                             const bool need_validation,
                             shared_ptr<RccGraph> sp_graph,
                             TxnOutput *output) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  verify(0);
  /*
  Log_debug("committing dtxn %" PRIx64, cmd_id);
//  if (RandomGenerator::rand(1, 2000) <= 1)
//    Log_info("on commit graph size: %d", graph.size());
  auto sp_tx = dynamic_pointer_cast<RccTx>(GetOrCreateTx(cmd_id));
  auto dtxn = dynamic_pointer_cast<TxTroad>(sp_tx);
  verify(rank == dtxn->current_rank_);
  verify(dtxn->p_output_reply_ == nullptr);
  dtxn->p_output_reply_ = output;
  dtxn->__debug_commit_received_ = true;
  verify(!dtxn->IsAborted());
  bool weird = dtxn->HasLogApplyStarted();
  Coroutine::CreateRun([sp_tx, weird](){
    auto sp_e = Reactor::CreateSpEvent<Event>();
    sp_e->test_ = [sp_tx] (int v) -> bool {
      return sp_tx->local_validated_->is_set_;
    };
    sp_e->Wait(120*1000*1000);
    if (sp_e->status_ == Event::TIMEOUT) {
      verify(!weird);
      verify(0);
    }
  });
  dtxn->commit_received_.Set(1);
// TODO XXXX whyyyyyyyyyyyyyyy?
  if (weird) {
    verify(0);
    verify(dtxn->status() > TXN_CMT);
    dtxn->__DebugCheckParents();
    return SUCCESS;
  }
//    dtxn->commit_request_received_ = true;
  if (sp_graph) {
    Aggregate(*sp_graph);
  }
  UpgradeStatus(*dtxn, TXN_CMT);
  WaitUntilAllPredecessorsAtLeastCommitting(dtxn.get());
  RccScc& scc = FindSccPred(*dtxn);
  Decide(scc);
  WaitUntilAllPredSccExecuted(scc);
  if (FullyDispatched(scc, rank) && !IsExecuted(scc, rank)) {
    Execute(scc);
  }
    // TODO verify by a wait time.
//    dtxn->sp_ev_commit_->Wait(1*1000*1000);
//    dtxn->sp_ev_commit_->Wait();
//    verify(dtxn->sp_ev_commit_->status_ != Event::TIMEOUT);
//    ret = dtxn->local_validation_result_ > 0 ? SUCCESS : REJECT;
//  dtxn->CommitRank();
  return SUCCESS;
*/
}

TroadCommo *SchedulerTroad::commo() {

  auto commo = dynamic_cast<TroadCommo *>(commo_);
  verify(commo != nullptr);
  return commo;
}
