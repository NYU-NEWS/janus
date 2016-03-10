#include "all.h"


rrr::PollMgr *svr_poll_mgr_g = nullptr;

namespace rococo {

static pthread_t th_id_s = 0;

RococoProxy *RccGraph::get_server_proxy(uint32_t id) {
  pthread_t th_id = pthread_self();
  if (th_id_s == 0) {
    th_id_s = th_id;
  } else {
    verify(th_id_s == th_id);
  }

  verify(id < rpc_proxies_.size());
  RococoProxy *rpc_proxy = rpc_proxies_[id];
  if (rpc_proxy == nullptr) {
    verify(svr_poll_mgr_g != nullptr);
    rrr::PollMgr *rpc_poll = svr_poll_mgr_g;
    auto &addr = server_addrs_[id];
    rrr::Client *rpc_cli = new rrr::Client(rpc_poll);
    int ret = rpc_cli->connect(addr.c_str());
    verify(ret == 0);
    rpc_proxy = new RococoProxy(rpc_cli);
    rpc_clients_[id] = (rpc_cli);
    rpc_proxies_[id] = (rpc_proxy);
  }
  return rpc_proxy;
}

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

uint64_t RccGraph::sub_txn_graph(uint64_t tid,
                                 GraphMarshaler &gra_m) {
  gra_m.gra = &txn_gra_;

  Vertex<TxnInfo> *source = txn_gra_.FindV(tid);
  verify(source != NULL);
  // Log::debug("compute for sub graph, tid: %llx parent size: %d",
  //     tid, (int) source->from_.size());

  auto &ret_set = gra_m.ret_set;

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

} // namespace rcc
