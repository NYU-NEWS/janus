#include "all.h"


rrr::PollMgr *svr_poll_mgr_g = nullptr;

namespace rococo {

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
void RccGraph::start_pie(txnid_t txn_id,
                         Vertex<TxnInfo> **tv) {
  verify(tv != NULL);
  *tv = txn_gra_.FindOrCreateV(txn_id);
  static auto id = Config::GetConfig()->get_site_id();
  auto txn_info = (*tv)->data_;
  verify(txn_info != nullptr);
  txn_info->servers_.insert(id);
}

uint64_t RccGraph::MinItfrGraph(uint64_t tid,
                                RccGraph &gra_m) {
//  gra_m.gra = &txn_gra_;

  Vertex<TxnInfo> *source = txn_gra_.FindV(tid);
  verify(source != NULL);
  // Log_debug("compute for sub graph, tid: %llx parent size: %d",
  //     tid, (int) source->from_.size());

//  auto &ret_set = gra_m.ret_set;
  verify(0); // TODO
  std::unordered_set<Vertex<TxnInfo> *> ret_set;
  find_txn_anc_opt(source, ret_set);
  ret_set.insert(source);
  return ret_set.size();
}

void RccGraph::find_txn_anc_opt(Vertex<TxnInfo> *source,
                                std::unordered_set<Vertex<TxnInfo> *> &ret_set) {
  verify(source != NULL);
  // Log::debug("compute for sub graph, tid: %llx parent size: %d",
  //     tid, (int) source->from_.size());
  std::vector<Vertex<TxnInfo> *> search_stack;
  search_stack.push_back(source);

  while (search_stack.size() > 0) {
    Vertex<TxnInfo> *v = search_stack.back();
    search_stack.pop_back();

    for (auto &kv: v->incoming_) {
      auto &parent = kv.first;
      if (!parent->data_->is_commit()
          && ret_set.find(parent) == ret_set.end()) {
        ret_set.insert(parent);
        search_stack.push_back(parent);
      }
    }
  }
  // remove source from result set.
  ret_set.erase(source);
  //if (RandomGenerator::rand(1, 100) == 1) {
  //    Log::info("anc size: %d", ret_set.size());
  //}
}

void RccGraph::find_txn_anc_opt(uint64_t txn_id,
                                std::unordered_set<Vertex<TxnInfo> *> &ret_set) {
  Vertex<TxnInfo> *source = txn_gra_.FindV(txn_id);
  verify(source != NULL);
  find_txn_anc_opt(source, ret_set);
}

void RccGraph::find_txn_scc_anc_opt(
    uint64_t txn_id,
    std::unordered_set<Vertex<TxnInfo> *> &ret_set
) {
  std::vector<Vertex<TxnInfo> *> scc = txn_gra_.FindSCC(txn_id);
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

void RccGraph::write_to_marshal(rrr::Marshal &m) const {
  verify(0); // TODO
  std::unordered_set<Vertex<TxnInfo> *> ret_set;
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


} // namespace rcc
