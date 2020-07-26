#include "../__dep__.h"
#include "../constants.h"
#include "dep_graph.h"
#include "server.h"

namespace janus {

shared_ptr<RccTx> RccGraph::FindOrCreateRccVertex(txnid_t txn_id,
                                                  RccServer *sched) {
  verify(sched != nullptr);
  auto v = FindOrCreateV(txn_id);
  RccTx &dtxn = *v;
  // FIXME, set mgr and mdb_txn?
  dtxn.sched_ = sched;
  return v;
}

void RccGraph::RemoveVertex(txnid_t txn_id) {
  Remove(txn_id);
}

void RccGraph::SelectGraphCmtUkn(RccTx& dtxn,
                                 shared_ptr<RccGraph> new_graph) {
  verify(0);
/*
  auto new_v = new_graph->FindOrCreateV(dtxn.id());
  auto s = dtxn.status();
  if (s >= TXN_DCD) {
    new_v->union_status(TXN_CMT);
  } else {
    new_v->union_status(s);
  }
  new_v->partition_ = dtxn.partition_;
  // TODO, verify that the parent vertex still exists.
  // TODO, verify that the new parent has the correct epoch.
  auto& parents = dtxn.parents_;
  for (auto &pair : parents) {
    verify(parents.size() > 0);
    auto parent_v = FindV(pair.first);
//    auto weight = pair.second;
    auto new_parent_v = new_graph->FindOrCreateV(parent_v->id());
    new_parent_v->partition_ = parent_v->partition_;
    verify(new_parent_v->status() == TXN_UKN);
    new_v->AddParentEdge(new_parent_v, 0);
  }
#ifdef DEBUG_CODE
  if (new_v->Get().status() >= TXN_CMT) {
    auto& p1 = new_v->GetParentSet();
    auto& p2 = vertex->GetParentSet();
    verify(p1 == p2);
  }
#endif
*/
}

void RccGraph::SelectGraph(set<shared_ptr<RccTx>> vertexes,
                           RccGraph *new_graph) {
/**
  for (auto v : vertexes) {
    auto new_v = new_graph->FindOrCreateV(*v);
    for (auto &kv: v->parents_) {
      auto parent_v = FindV(kv.first);
      auto weight = 0;
      shared_ptr<RccTx> new_parent_v = nullptr;
      if (vertexes.count(parent_v) > 0) {
        new_parent_v = new_graph->FindOrCreateV(*parent_v);
      } else {
        new_parent_v = new_graph->FindOrCreateV(parent_v->id());
        new_parent_v->partition_ = parent_v->partition_;
        verify(new_parent_v->status() == TXN_UKN);
      }
      new_v->AddParentEdge(new_parent_v, weight);
    }
#ifdef DEBUG_CODE
    if (v->Get().status() >= TXN_CMT) {
      auto& p1 = v->GetParentSet();
      auto& p2 = v->GetParentSet();
      verify(p1 == p2);
    }
#endif
  }

#ifdef DEBUG_CODE
  for (auto& pair : new_graph->vertex_index_) {
    RccVertex* rhs_v = pair.second;
    RccVertex* source = FindV(rhs_v->id());
    if (vertexes.count(source) > 0 && source->Get().status() >= TXN_CMT) {
      RccVertex* target = new_graph->FindV(source->id());
      verify(target == rhs_v);
      verify(target);
      auto p1 = source->GetParentSet();
      auto p2 = target->GetParentSet();
      verify(p1 == p2);
    }
  }
#endif
*/
}

uint64_t RccGraph::MinItfrGraph(RccTx& tx,
                                shared_ptr<RccGraph> new_graph,
                                bool quick,
                                int depth) {
  verify(0);
  return 0;
/**
  verify(new_graph);
  if (tx.parents_.size() == 0 && quick) {
    new_graph->empty_ = true;
    return 0;
  }
  verify(depth == 1);
  if (true) {
    SelectGraphCmtUkn(tx, new_graph);
  } else if (tx.status() >= TXN_DCD) {
//    RccScc& scc = FindSCC(source);
//    set<RccDTxn*> vertex_set{};
//    vertex_set.insert(scc.begin(), scc.end());
//    SelectGraph(vertex_set, new_graph);
//#ifdef DEBUG_CODE
//    verify(new_graph->size() >= scc.size());
//#endif
//  } else if (depth == 1) {
//    set<RccDTxn*> vertex_set{source};
//    SelectGraph(vertex_set, new_graph);
//  } else {
//    verify(0);
//// Log_debug("compute for sub graph, tid: %llx parent size: %d",
////     tid, (int) source->from_.size());
////  auto &ret_set = gra_m.ret_set;
////  unordered_set<RccVertex *> ret_set;
////  find_txn_anc_opt(source, ret_set);
//    vector<RccDTxn *> search_stack;
//    set<RccDTxn*> searched_set;
//    search_stack.push_back(source);
//    verify(depth == 1 || depth == -1);
//    while (search_stack.size() > 0) {
//      RccDTxn *v = search_stack.back();
//      searched_set.insert(v);
//      search_stack.pop_back();
//      RccDTxn* new_v = new_graph->FindOrCreateV(*v);
//      for (auto &kv: v->incoming_) {
//        auto parent_v = kv.first;
//        auto weight = kv.second;
//        RccDTxn& parent_txn = *parent_v;
//        if (parent_v == v) {
//          verify(0); // or continue?
//        }
////        if (parent_txn.status() >= TXN_DCD || parent_txn.IsExecuted()) {
////          continue;
////        }
//        if (searched_set.find(parent_v) != searched_set.end()) {
//          continue;
//        }
//        if (depth == 1) {
//          // TODO
//          RccDTxn* new_parent_v = new_graph->FindOrCreateV(parent_v->id());
//          RccDTxn& tinfo = *new_parent_v;
//          tinfo.partition_ = parent_v->partition_;
//          verify(tinfo.status() == TXN_UKN);
//          new_v->AddParentEdge(new_parent_v, weight);
//        } else if (depth == -1) {
//          verify(0);
//          RccDTxn* new_parent_v = new_graph->FindOrCreateV(*parent_v);
//          new_v->AddParentEdge(new_parent_v, weight);
//          search_stack.push_back(parent_v);
//        } else {
//          verify(0);
//        }
//      }
//    }
  }

#ifdef DEBUG_CODE
  for (auto& pair : new_graph->vertex_index_) {
    RccVertex* rhs_v = pair.second;
    RccVertex* source = FindV(rhs_v->id());
    if (rhs_v->Get().status() >= TXN_CMT) {
      RccVertex* target = new_graph->FindV(source->id());
      verify(target == rhs_v);
      verify(target);
      auto p1 = source->GetParentSet();
      auto p2 = target->GetParentSet();
      verify(p1 == p2);
    }
  }
#endif

  auto sz = new_graph->size();
//  Log_debug("return graph size: %llx", sz);
//  verify(new_graph->FindV(tid) != nullptr);
  return sz;
*/
}

bool RccGraph::operator==(RccGraph &rhs) const {
  // quick check on vertex_index size
  if (const_cast<RccGraph *>(this)->vertex_index().size()
      != rhs.vertex_index().size())
    return false;
  for (auto &pair: const_cast<RccGraph *>(this)->vertex_index()) {
    auto id = pair.first;
    auto vertex = pair.second;
    auto it = rhs.vertex_index().find(id);
    if (it == rhs.vertex_index().end())
      return false;
    auto av = it->second;
    if (*vertex != *av)
      return false;
  }
  //

  return true;
}

void RccGraph::RebuildEdgePointer(map<txnid_t, shared_ptr<RccTx>> &index) {
  // TODO
  for (auto &pair : index) {
    auto id = pair.first;
    auto v = pair.second;
    // add pointers
//    for (auto& parent_id : v->parents_) {
//      RccDTxn* parent_v = FindV(parent_id);
//      verify(parent_v);
//      v->incoming_[parent_v] = EDGE_D; // FIXME
//      parent_v->outgoing_[v] = EDGE_D; // FIXME
//    }
// //    remove pointers
//    for (auto it = v->incoming_.begin(); it != v->incoming_.end();) {
//      RccDTxn* parent_v = it->first;
//      auto parent_id = parent_v->id();
//      if (v->parents_.count(parent_id) == 0) {
//        it = v->incoming_.erase(it);
//        auto n = parent_v->outgoing_.erase(v); // FIXME
//        parent_v->removed_children_.insert(v);
//        verify(n > 0);
//      } else {
//        it++;
//      }
//    }
//    verify(v->parents_.size() == v->incoming_.size());
  }
}

void RccGraph::UpgradeStatus(RccTx& v, int rank, int8_t status) {
  verify(rank == RANK_D || rank == RANK_I);
  auto s = v.subtx(rank).status();
  if (s >= TXN_CMT) {
    v.__DebugCheckParents(rank);
//    RccServer::__DebugCheckParentSetSize(v.id(), v.parents_.size());
  } else if (status >= TXN_CMT) {
//    RccServer::__DebugCheckParentSetSize(v.id(), v.parents_.size());
//    v.__DebugCheckParents();
  }
  v.subtx(rank).union_status(status);
  v.__DebugCheckParents(rank);
}

shared_ptr<RccTx> RccGraph::AggregateVertex(shared_ptr<RccTx> rhs_dtxn) {
  return rhs_dtxn;
  verify(0);
  /**
  // create the dtxn if not exist.
  auto lhs_dtxn = FindOrCreateV(*rhs_dtxn);
  verify(0);
//  auto status1 = lhs_dtxn->status();
//  auto status2 = rhs_dtxn->status();
  auto &parent_set1 = lhs_dtxn->parents_;
  auto &parent_set2 = rhs_dtxn->GetParents();
#ifdef DEBUG_CODE
  if (status1 >= TXN_CMT) {
    RccSched::__DebugCheckParentSetSize(vertex->id(), vertex->parents_.size());
  }
  if (status2 >= TXN_CMT) {
//    Log_info("aggregating gt CMT, txnid: %llx, parent size: %d",
//             rhs_v->id(), (int) rhs_v->parents_.size());
    RccSched::__DebugCheckParentSetSize(rhs_v->id(), rhs_v->parents_.size());
  }
  if (status1 >= TXN_CMT && status2 >= TXN_CMT) {
    // they should have the same parents.
    if (parent_set1 != parent_set2) {
      Log_fatal("failed in aggregating, txnid: %llx, parent size: %d",
                rhs_v->id(), (int) rhs_v->parents_.size());
      verify(0);
    }
  }
#endif
//  If local vertex is not yet fully dispatched, pre-setting its dependencies does not hurt
//  if (lhs_dtxn->shared_rank_ > rhs_dtxn->shared_rank_) {
//    verify(0);
//    return lhs_dtxn;
//  } else if (lhs_dtxn->shared_rank_ < rhs_dtxn->shared_rank_) {
//    lhs_dtxn->parents_ = rhs_dtxn->parents_;
//    lhs_dtxn->shared_rank_ = rhs_dtxn->shared_rank_;
//    lhs_dtxn->max_seen_ballot_ = rhs_dtxn->max_accepted_ballot_;
//    lhs_dtxn->max_accepted_ballot_ = rhs_dtxn->max_accepted_ballot_;
//  } else {
    if (lhs_dtxn->max_seen_ballot_ == 0 &&
        status1 <= TXN_STD &&
        status2 <= TXN_STD) {
      verify(0);
//      lhs_dtxn->parents_.insert(rhs_dtxn->parents_.begin(),
//                                rhs_dtxn->parents_.end());
//    lhs_dtxn->partition_.insert(rhs_dtxn->partition_.begin(),
//                                rhs_dtxn->partition_.end());
    } else if (status2 >= TXN_CMT) {
      if (status2 > status1) {
        lhs_dtxn->parents_ = rhs_dtxn->parents_;
      } // else do nothing.
    } else {
      if (status2 >= TXN_PAC && status2 >= status1) {
        if (lhs_dtxn->max_seen_ballot_ <= rhs_dtxn->max_accepted_ballot_) {
          lhs_dtxn->max_seen_ballot_ = rhs_dtxn->max_accepted_ballot_;
          lhs_dtxn->parents_ = rhs_dtxn->parents_;
        } // else do nothing.
      }
    }
//  }
#ifdef DEBUG_CODE
  if (status1 >= TXN_CMT) {
    // do nothing about edges.
    if (status2 >= TXN_DCD) {
      for (auto p : parent_set2) {
        verify(parent_set1.count(p) > 0);
      }
    }
  }
#endif
  lhs_dtxn->partition_.insert(rhs_dtxn->partition_.begin(),
                              rhs_dtxn->partition_.end());
  // TODO add rank support here.
  lhs_dtxn->status_.Set(lhs_dtxn->status_.value_ |= rhs_dtxn->status_.value_);
  lhs_dtxn->__DebugCheckParents();
  rhs_dtxn->__DebugCheckParents();
  return lhs_dtxn;
  **/
}

//RccScc& RccGraph::FindSCC(RccDTxn *vertex) {
//  RccScc& scc2 = Graph<RccDTxn>::FindSccPred(vertex);
//#ifdef DEBUG_CODE
//  RccScc& scc = Graph<TxnInfo>::FindSCC(vertex);
//  std::sort(scc.begin(), scc.end());
//  std::sort(scc2.begin(), scc2.end());
//  verify(scc == scc2);
//  verify(vertex->Get().status() >= TXN_CMT);
//  for (RccVertex* v : scc) {
//    verify(v->Get().status() >= TXN_CMT);
//    verify(AllAncCmt(v));
//  }
//#endif
//  return scc2;
//}

bool RccGraph::AllAncCmt(shared_ptr<RccTx> vertex) {
  verify(0);
/*
  if (vertex->all_anc_cmt_hint) {
    return true;
  }
  bool all_anc_cmt = true;
  std::function<int(RccTx&)> func = [&all_anc_cmt](RccTx& v) -> int {
    RccTx &info = v;
    int r = 0;
    if (info.HasLogApplyStarted() ||
        info.all_anc_cmt_hint ||
        info.IsAborted()) {
      r = RccGraph::SearchHint::Skip;
    } else if (info.status() >= TXN_CMT) {
      r = RccGraph::SearchHint::Ok;
    } else {
      r = RccGraph::SearchHint::Exit;
      all_anc_cmt = false;
    }
    return r;
  };
  TraversePred(*vertex, -1, func);
  vertex->all_anc_cmt_hint = all_anc_cmt;
  return all_anc_cmt;
*/
}

map<txnid_t, shared_ptr<RccTx>> RccGraph::Aggregate(epoch_t epoch,
                                                    RccGraph &graph) {
  // aggregate vertexes
  map<txnid_t, shared_ptr<RccTx>> index;
  auto& vidx = graph.vertex_index();
  for (const auto& pair: vidx) {
    auto rhs_v = pair.second;
    verify(pair.first == rhs_v->id());
    auto vertex = AggregateVertex(rhs_v);
    RccTx &dtxn = *vertex;
    if (dtxn.epoch_ == 0) {
      dtxn.epoch_ = epoch;
    }
    if (sched_) {
      sched_->epoch_mgr_.AddToEpoch(dtxn.epoch_, dtxn.tid_);
    }
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

bool RccGraph::HasICycle(const RccScc &scc) {
  verify(0);
//  for (auto& vertex : scc) {
//    set<RccDTxn *> walked;
//    bool ret = false;
//    std::function<bool(RccDTxn *)> func =
//        [&ret, vertex](RccDTxn *v) -> bool {
//          if (v == vertex) {
//            ret = true;
//            return false;
//          }
//          return true;
//        };
//    TraverseDescendant(vertex, -1, func, walked, EDGE_I);
//    if (ret) return true;
//  }
//  return false;
};

} // namespace janus
