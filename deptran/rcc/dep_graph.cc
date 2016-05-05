#include "../__dep__.h"
#include "../constants.h"
#include "dep_graph.h"



namespace rococo {

rrr::PollMgr *svr_poll_mgr_g = nullptr;
static pthread_t th_id_s = 0;

//RococoProxy *RccGraph::get_server_proxy(uint32_t id) {
//  pthread_t th_id = pthread_self();
//  if (th_id_s == 0) {
//    th_id_s = th_id;
//  } else {
//    verify(th_id_s == th_id);
//  }
//
//  verify(id < rpc_proxies_.size());
//  RococoProxy *rpc_proxy = rpc_proxies_[id];
//  if (rpc_proxy == nullptr) {
//    verify(svr_poll_mgr_g != nullptr);
//    rrr::PollMgr *rpc_poll = svr_poll_mgr_g;
//    auto &addr = server_addrs_[id];
//    rrr::Client *rpc_cli = new rrr::Client(rpc_poll);
//    int ret = rpc_cli->connect(addr.c_str());
//    verify(ret == 0);
//    rpc_proxy = new RococoProxy(rpc_cli);
//    rpc_clients_[id] = (rpc_cli);
//    rpc_proxies_[id] = (rpc_proxy);
//  }
//  return rpc_proxy;
//}

/** on start_req */
void RccGraph::FindOrCreateTxnInfo(txnid_t txn_id,
                                   RccVertex **tv) {
  verify(tv != NULL);
  *tv = FindOrCreateV(txn_id);
  verify(FindV(txn_id) != nullptr);
  verify(*tv != nullptr);
  // TODO fix.
  TxnInfo& txn_info = (*tv)->Get();
//  verify(txn_info != nullptr);
  txn_info.partition_.insert(partition_id_);
  txn_info.graph_ = this;
}

void RccGraph::SelectGraph(set<RccVertex*> vertexes, RccGraph* new_graph) {
  for (auto v : vertexes) {
    RccVertex* new_v = new_graph->FindOrCreateV(*v);
    for (auto &kv: v->incoming_) {
      RccVertex* parent_v = kv.first;
      auto weight = kv.second;
      RccVertex* new_parent_v = nullptr;
      if (vertexes.count(parent_v) > 0) {
        new_parent_v = new_graph->FindOrCreateV(*parent_v);
      } else {
        new_parent_v = new_graph->FindOrCreateV(parent_v->id());
        verify(new_parent_v->Get().status() == TXN_UKN);
      }
      new_v->AddParentEdge(new_parent_v, weight);
    }
#ifdef DEBUG_CODE
    if (v->Get().status() >- TXN_CMT) {
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
}

uint64_t RccGraph::MinItfrGraph(uint64_t tid,
                                RccGraph* new_graph,
                                bool quick,
                                int depth) {
  verify(new_graph != nullptr);
//  gra_m.gra = &txn_gra_;
  Vertex<TxnInfo> *source = FindV(tid);
  verify(source != nullptr);
  // quick path
  if (source->incoming_.size() == 0 && quick) {
    return 0;
  }
  verify(depth == 1);
  if (source->Get().get_status() >= TXN_DCD) {
    RccScc& scc = FindSCC(source);
    set<RccVertex*> vertex_set{};
    vertex_set.insert(scc.begin(), scc.end());
    SelectGraph(vertex_set, new_graph);
#ifdef DEBUG_CODE
    verify(new_graph->size() >= scc.size());
#endif
  } else if (depth == 1) {
    set<RccVertex*> vertex_set{source};
    SelectGraph(vertex_set, new_graph);
  } else {
// Log_debug("compute for sub graph, tid: %llx parent size: %d",
//     tid, (int) source->from_.size());
//  auto &ret_set = gra_m.ret_set;
//  unordered_set<RccVertex *> ret_set;
//  find_txn_anc_opt(source, ret_set);
    vector<RccVertex *> search_stack;
    set<RccVertex*> searched_set;
    search_stack.push_back(source);
    verify(depth == 1 || depth == -1);
    while (search_stack.size() > 0) {
      RccVertex *v = search_stack.back();
      searched_set.insert(v);
      search_stack.pop_back();
      RccVertex* new_v = new_graph->FindOrCreateV(*v);
      for (auto &kv: v->incoming_) {
        auto parent_v = kv.first;
        auto weight = kv.second;
        TxnInfo& parent_txn = parent_v->Get();
        if (parent_v == v) {
          verify(0); // or continue?
        }
//        if (parent_txn.status() >= TXN_DCD || parent_txn.IsExecuted()) {
//          continue;
//        }
        if (searched_set.find(parent_v) != searched_set.end()) {
          continue;
        }
        if (depth == 1) {
          // TODO
          RccVertex* new_parent_v = new_graph->FindOrCreateV(parent_v->id());
          TxnInfo& tinfo = new_parent_v->Get();
          tinfo.partition_ = parent_v->Get().partition_;
          verify(tinfo.status() == TXN_UKN);
          new_v->AddParentEdge(new_parent_v, weight);
        } else if (depth == -1) {
          verify(0);
          RccVertex* new_parent_v = new_graph->FindOrCreateV(*parent_v);
          new_v->AddParentEdge(new_parent_v, weight);
          search_stack.push_back(parent_v);
        } else {
          verify(0);
        }
      }
    }
  }
//
//#ifdef DEBUG_CODE
//  for (auto& pair : new_graph->vertex_index_) {
//    RccVertex* rhs_v = pair.second;
//    RccVertex* source = FindV(rhs_v->id());
//    if (source->Get().status() >= TXN_CMT) {
//      RccVertex* target = new_graph->FindV(source->id());
//      verify(target == rhs_v);
//      verify(target);
//      auto p1 = source->GetParentSet();
//      auto p2 = target->GetParentSet();
//      verify(p1 == p2);
//    }
//  }
//#endif

  auto sz = new_graph->size();
  Log_debug("return graph size: %llx", sz);
  return sz;
}

bool RccGraph::operator== (RccGraph& rhs) const {
  // quick check on vertex_index size
  if (vertex_index_.size() != rhs.vertex_index_.size())
    return false;
  for (auto& pair: vertex_index_) {
    auto id = pair.first;
    RccVertex* vertex = pair.second;
    auto it = rhs.vertex_index_.find(id);
    if (it == rhs.vertex_index_.end())
      return false;
    RccVertex* av = it->second;
    if (*vertex != *av)
      return false;
  }
  //

  return true;
}

void RccGraph::RebuildEdgePointer(map<txnid_t, RccVertex*>& index) {
  // TODO
  for (auto& pair : index) {
    auto id = pair.first;
    RccVertex* v = pair.second;
    // add pointers
    for (auto& parent_id : v->parents_) {
      RccVertex* parent_v = FindV(parent_id);
      verify(parent_v);
      v->incoming_[parent_v] = EDGE_D; // FIXME
      parent_v->outgoing_[v] = EDGE_D; // FIXME
    }
    // remove pointers
    for (auto it = v->incoming_.begin(); it != v->incoming_.end();) {
      RccVertex* parent_v = it->first;
      auto parent_id = parent_v->id();
      if (v->parents_.count(parent_id) == 0) {
        it = v->incoming_.erase(it);
        auto n = parent_v->outgoing_.erase(v); // FIXME
        verify(n > 0);
      } else {
        it++;
      }
    }
    verify(v->parents_.size() == v->incoming_.size());
  }
}

void RccGraph::BuildEdgePointer(RccGraph &graph,
                                map<txnid_t, RccVertex*>& index) {
  verify(0);
  for (auto &pair: graph.vertex_index_) {
    auto id = pair.first;
    RccVertex* a_vertex = pair.second;
    RccVertex* vertex = index[a_vertex->id()];

#ifdef DEBUG_CODE
    if (vertex->Get().get_status() >= TXN_CMT &&
        a_vertex->Get().get_status() >= TXN_CMT) {
      // they should have the same parents.
      auto set1 = vertex->GetParentSet();
      auto set2 = a_vertex->GetParentSet();
      verify(set1 == set2);
    }
#endif
    if (vertex->Get().status() < TXN_CMT &&
        a_vertex->Get().status() >= TXN_CMT) {
      for (auto pair : vertex->incoming_) {
        RccVertex* parent_v = pair.first;
        parent_v->outgoing_.erase(vertex); // FIXME check wait list still?
      }
      vertex->incoming_.clear();
    }

    if (vertex->Get().status() >= TXN_CMT &&
        a_vertex->Get().status() < TXN_CMT) {
      // do nothing
    } else {
      for (auto pair : a_vertex->incoming_) {
        auto a_parent_vertex = pair.first;
        auto weight = pair.second;
        auto parent = index[a_parent_vertex->id()];
        vertex->incoming_[parent] |= weight;
        parent->outgoing_[vertex] |= weight;
      }
    }
  }
}

RccVertex* RccGraph::AggregateVertex(RccVertex *rhs_v) {
  // create the dtxn if not exist.
  RccVertex* vertex = FindOrCreateV(*rhs_v);
  auto status1 = vertex->Get().get_status();
  auto status2 = rhs_v->Get().get_status();
  auto& parent_set1 = vertex->GetParentSet();
  auto& parent_set2 = rhs_v->GetParentSet();
#ifdef DEBUG_CODE
  if (status2 >= TXN_CMT) {
//    Log_info("aggregating gt CMT, txnid: %llx, parent size: %d",
//             rhs_v->id(), (int) rhs_v->parents_.size());
  }
  if (status1 >= TXN_CMT && status2 >= TXN_CMT) {
    // they should have the same parents.
    if (parent_set1 != parent_set2) {
//      Log_fatal("failed in aggregating, txnid: %llx, parent size: %d",
//                rhs_v->id(), (int) rhs_v->parents_.size());
      verify(0);
    }
  }
#endif

  if (status1 < TXN_CMT && status2 < TXN_CMT) {
    vertex->parents_.insert(rhs_v->parents_.begin(), rhs_v->parents_.end());
  }
  if (status1 < TXN_CMT && status2 >= TXN_CMT) {
    vertex->parents_ = rhs_v->parents_;
  }
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
  TxnInfo &info = vertex->Get();
  TxnInfo &a_info = rhs_v->Get();
  info.union_data(a_info); // TODO
  return vertex;
}

void RccGraph::Aggregate(RccGraph &graph) {
  // aggregate vertexes
  map<txnid_t, RccVertex*> index;
  for (auto& pair: graph.vertex_index_) {
    RccVertex* rhs_v = pair.second;
    verify(pair.first == rhs_v->id());
    RccVertex* vertex = AggregateVertex(rhs_v);
    verify(vertex->id() == pair.second->id());
    verify(vertex_index_.count(vertex->id()) > 0);
    index[vertex->id()] = vertex;
  }
  // aggregate edges.
  RebuildEdgePointer(index);

#ifdef DEBUG_CODE
  for (auto& pair: graph.vertex_index_) {
    // TODO optimize here.
    auto txnid = pair.first;
    RccVertex* rhs_v = pair.second;
    auto v = FindV(txnid);
    verify(v != nullptr);
    // TODO, check the Sccs are the same.
    if (rhs_v->Get().status() >= TXN_DCD) {
      verify(v->Get().status() >= TXN_DCD);
      RccScc& rhs_scc = graph.FindSCC(rhs_v);
      for (auto vv : rhs_scc) {
        verify(vv->Get().status() >= TXN_DCD);
      }
      RccScc& scc = FindSCC(v);
      auto sz = scc.size();
      auto rhs_sz = rhs_scc.size();
      if (sz != rhs_sz) {
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
        bool r = std::any_of(scc.begin(),
                             scc.end(),
                             [vv] (RccVertex* vvv) -> bool {
                               return vvv->id() == vv->id();
                             }
        );
        verify(r);
      }
    }

    if (v->Get().status() >= TXN_DCD) {
      RccScc& scc = FindSCC(v);
      for (auto vv : scc) {
        auto s = vv->Get().status();
        verify(s >= TXN_DCD);
      }
    }
  }

#endif
//  this->BuildEdgePointer(graph, index);
}


void RccGraph::find_txn_anc_opt(RccVertex *source,
                                unordered_set<RccVertex *> &ret_set) {
  verify(0);
//  verify(source != NULL);
//  verify(ret_set.size() == 0);
//  // Log::debug("compute for sub graph, tid: %llx parent size: %d",
//  //     tid, (int) source->from_.size());
//  vector<RccVertex *> search_stack;
//  search_stack.push_back(source);
//
//  while (search_stack.size() > 0) {
//    RccVertex *v = search_stack.back();
//    search_stack.pop_back();
//
//    for (auto &kv: v->incoming_) {
//      auto &parent_vertex = kv.first;
//      TxnInfo &parent_txn = parent_vertex->data_;
//      if (!parent_txn.IsDecided() &&
//          ret_set.find(parent_vertex) == ret_set.end()) {
//        ret_set.insert(parent_vertex);
//        search_stack.push_back(parent_vertex);
//      }
//    }
//  }
  // remove source from result set. ? no
  // ret_set.erase(source);
  //if (RandomGenerator::rand(1, 100) == 1) {
  //    Log::info("anc size: %d", ret_set.size());
  //}
}

bool RccGraph::HasICycle(const RccScc& scc) {
  for (auto& vertex : scc) {
    set<RccVertex *> walked;
    bool ret = false;
    std::function<bool(RccVertex *)> func =
        [&ret, vertex](RccVertex *v) -> bool {
          if (v == vertex) {
            ret = true;
            return false;
          }
          return true;
        };
    TraverseDescendant(vertex, -1, func, walked, EDGE_I);
    if (ret) return true;
  }
  return false;
};

void RccGraph::find_txn_anc_opt(uint64_t txn_id,
                                std::unordered_set<Vertex<TxnInfo> *> &ret_set) {
  Vertex<TxnInfo> *source = FindV(txn_id);
  verify(source != NULL);
  find_txn_anc_opt(source, ret_set);
}

void RccGraph::find_txn_scc_anc_opt(
    uint64_t txn_id,
    std::unordered_set<Vertex<TxnInfo> *> &ret_set
) {
  std::vector<Vertex<TxnInfo> *> scc = FindSCC(txn_id);
  //for (auto v: scc) {
  //    find_txn_anc_opt(v->data_.id(), ret_set);
  //}
  //
  verify(scc.size() > 0);
  auto &v = scc[0];
  find_txn_anc_opt(v, ret_set);

  for (auto &scc_v: scc) {
    ret_set.erase(scc_v);
  }

  //if (RandomGenerator::rand(1, 100) == 1) {
  //    Log::info("scc anc size: %d", ret_set.size());
  //}
}


void RccGraph::write_to_marshal(rrr::Marshal &m) const {
  verify(0); // TODO
//  unordered_set<RccVertex *> ret_set;
//  int32_t n = size();
//  m << n;
//
//  for (auto &old_sv: ret_set) {
//    m << old_sv->data_->id();
//    m << *(old_sv->data_);
//
//    marshal_help_1(m, ret_set, old_sv);
//
////        marshal_help_2(m, ret_set, old_sv);
//
////      verify(ma_size == to_size);
//  }

  //if (RandomGenerator::rand(1,200) == 1) {
  //    Log::info("sub graph in start reply, size: %d",  (int)n);
  //}
  //Log::debug("sub graph, return size: %d",  (int)n);
}


void RccGraph::marshal_help_1(rrr::Marshal &m,
                              const std::unordered_set<Vertex<TxnInfo>*> &ret_set,
                              Vertex<TxnInfo> *old_sv) const {
//  int32_t to_size = 0;
//  //if (RandomGenerator::rand(1,200) == 1) {
//  //    Log::info("direct parent number, size: %d",  (int)old_sv->to_.size());
//  //}
//  std::vector<Vertex<TxnInfo>*> to;
//  std::vector<int8_t> relation;
//  for (auto &kv: old_sv->outgoing_) {
//    auto old_tv = kv.first;
//    if (ret_set.find(old_tv) != ret_set.end()) {
//      to_size++;
//      to.push_back(kv.first);
//      relation.push_back(kv.second);
//    } else {
//      //Log::debug("this vertex is not what I want to include");
//    }
//  }
//  m << to_size;
//  for (int i = 0; i < to_size; i++) {
//    uint64_t id = to[i]->data_->id();
//    m << id;
//    m << relation[i];
//  }
}

void RccGraph::marshal_help_2(
    rrr::Marshal &m,
    const std::unordered_set<Vertex<TxnInfo>*> &ret_set,
    Vertex<TxnInfo> *old_sv) const {
  //int32_t ma_size = 0;
//  for (auto &kv: old_sv->outgoing_) {
//    auto old_tv = kv.first;
//    //int8_t relation = kv.second;
//    if (ret_set.find(old_tv) != ret_set.end()) {
//      //uint64_t id = old_tv->data_.id();
//      //m << id;
//      //m << relation;
//      //ma_size++;
//    } else {
//      //Log::debug("this vertex is not what I want to include");
//    }
//  }
}

//Marshal& RccGraph::ToMarshal(Marshal& m) const {
//  m << (Graph<TxnInfo>&)*this;
//}
//
//Marshal& RccGraph::FromMarshal(Marshal& m) {
//  m >> *this;
//}

} // namespace rcc
