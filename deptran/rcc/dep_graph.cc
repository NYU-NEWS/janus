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
  auto txn_info = (*tv)->data_;
  verify(txn_info != nullptr);
  txn_info->partition_.insert(partition_id_);
}

uint64_t RccGraph::MinItfrGraph(uint64_t tid,
                                RccGraph* new_graph) {
//  gra_m.gra = &txn_gra_;

  Vertex<TxnInfo> *source = FindV(tid);
  verify(source != NULL);
  // Log_debug("compute for sub graph, tid: %llx parent size: %d",
  //     tid, (int) source->from_.size());

//  auto &ret_set = gra_m.ret_set;
//  unordered_set<RccVertex *> ret_set;
//  find_txn_anc_opt(source, ret_set);

  vector<RccVertex *> search_stack;
  set<RccVertex*> searched_set;
  search_stack.push_back(source);
  bool debug = false;
  while (search_stack.size() > 0) {
    RccVertex *v = search_stack.back();
    searched_set.insert(v);
    search_stack.pop_back();
    RccVertex* new_v = new_graph->FindOrCreateV(*v);
    for (auto &kv: v->incoming_) {
      auto parent_v = kv.first;
      auto weight = kv.second;
      TxnInfo& parent_txn = *parent_v->data_;
      if (parent_v == v) {
        verify(0); // or continue?
      }
      if (parent_txn.IsDecided()) {
        continue;
      }
      if (searched_set.find(parent_v) != searched_set.end()) {
        continue;
      }
      // TODO Debug here. why is this not happening?
//      verify(0);
      debug = true;
      RccVertex* new_parent_v = new_graph->FindOrCreateV(*parent_v);
      new_v->AddParentEdge(new_parent_v, weight);
      search_stack.push_back(parent_v);
    }
  }
  auto sz = new_graph->size();
  if (debug) {
    Log_debug("return graph size: %llx", sz);
  }
  return sz;
}



void RccGraph::BuildEdgePointer(RccGraph &graph,
                                map<txnid_t, RccVertex*>& index) {
  for (auto &pair: graph.vertex_index_) {
    auto id = pair.first;
    auto a_vertex = pair.second;
    auto vertex = index[a_vertex->id()];
    for (auto pair : a_vertex->incoming_) {
      auto a_parent_vertex = pair.first;
      auto weight = pair.second;
      auto parent = index[a_parent_vertex->id()];
      vertex->incoming_[parent] |= weight;
      parent->outgoing_[vertex] |= weight;
    }
  }
}

RccVertex* RccGraph::AggregateVertex(RccVertex *av) {
  // create the dtxn if not exist.
  auto vertex = FindOrCreateV(*av);
  if (vertex->data_ == av->data_) {
    // skip
  } else {
    // add edges.
    TxnInfo &info = *vertex->data_;
    TxnInfo &a_info = *av->data_;
    info.union_data(a_info); // TODO
  }
  return vertex;
}

void RccGraph::Aggregate(RccGraph &graph) {
  // aggregate vertexes
  map<txnid_t, RccVertex*> index;
  for (auto& pair: graph.vertex_index_) {
    auto vertex = this->AggregateVertex(pair.second);
    index[vertex->id()] = vertex;
  }
  // aggregate edges.
  this->BuildEdgePointer(graph, index);
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
  unordered_set<RccVertex *> ret_set;
  int32_t n = size();
  m << n;

  for (auto &old_sv: ret_set) {
    m << old_sv->data_->id();
    m << *(old_sv->data_);

    marshal_help_1(m, ret_set, old_sv);

//        marshal_help_2(m, ret_set, old_sv);

//      verify(ma_size == to_size);
  }

  //if (RandomGenerator::rand(1,200) == 1) {
  //    Log::info("sub graph in start reply, size: %d",  (int)n);
  //}
  //Log::debug("sub graph, return size: %d",  (int)n);
}


void RccGraph::marshal_help_1(rrr::Marshal &m,
                              const std::unordered_set<Vertex<TxnInfo>*> &ret_set,
                              Vertex<TxnInfo> *old_sv) const {
  int32_t to_size = 0;
  //if (RandomGenerator::rand(1,200) == 1) {
  //    Log::info("direct parent number, size: %d",  (int)old_sv->to_.size());
  //}
  std::vector<Vertex<TxnInfo>*> to;
  std::vector<int8_t> relation;
  for (auto &kv: old_sv->outgoing_) {
    auto old_tv = kv.first;
    if (ret_set.find(old_tv) != ret_set.end()) {
      to_size++;
      to.push_back(kv.first);
      relation.push_back(kv.second);
    } else {
      //Log::debug("this vertex is not what I want to include");
    }
  }
  m << to_size;
  for (int i = 0; i < to_size; i++) {
    uint64_t id = to[i]->data_->id();
    m << id;
    m << relation[i];
  }
}

void RccGraph::marshal_help_2(
    rrr::Marshal &m,
    const std::unordered_set<Vertex<TxnInfo>*> &ret_set,
    Vertex<TxnInfo> *old_sv
) const {
  //int32_t ma_size = 0;
  for (auto &kv: old_sv->outgoing_) {
    auto old_tv = kv.first;
    //int8_t relation = kv.second;
    if (ret_set.find(old_tv) != ret_set.end()) {
      //uint64_t id = old_tv->data_.id();
      //m << id;
      //m << relation;
      //ma_size++;
    } else {
      //Log::debug("this vertex is not what I want to include");
    }
  }
}


} // namespace rcc
