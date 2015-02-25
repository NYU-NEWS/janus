
#include <pthread.h>

#include "all.h"


rrr::PollMgr *poll_mgr_g = nullptr;

namespace rococo {

static pthread_t th_id_s = 0;

RococoProxy* DepGraph::get_server_proxy(uint32_t id) {
    pthread_t th_id = pthread_self();
    if (th_id_s == 0) {
        th_id_s = th_id;
    } else {
        verify(th_id_s == th_id);
    }

    verify(id < rpc_proxies_.size());
    RococoProxy *rpc_proxy = rpc_proxies_[id];
    if (rpc_proxy == nullptr) {
        verify(poll_mgr_g != nullptr);
        rrr::PollMgr *rpc_poll = poll_mgr_g;
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

/**
 * find or insert.
 */
void DepGraph::foi_pie_txn(PieInfo &pi,
        Vertex<PieInfo> **pv,
        Vertex<TxnInfo> **tv) {
    *pv = pie_gra_.find_or_insert(pi);
    *tv = txn_gra_.find_or_insert(pi.txn_id_);

}

void DepGraph::foi_pie(PieInfo &pi,
        Vertex<PieInfo> **pv) {
    //*pv = pie_gra_.find_or_insert(pi);
    *pv = new Vertex<PieInfo>(pi);
}

void DepGraph::foi_txn(uint64_t &txn_id,
        Vertex<TxnInfo> **tv) {
    *tv = txn_gra_.find_or_insert(txn_id);
}

Vertex<TxnInfo> *DepGraph::start_pie_txn(uint64_t tid) {
    Vertex<TxnInfo> *tv;
    foi_txn(tid, &tv);
    static auto id = Config::get_config()->get_site_id();
    tv->data_.servers_.insert(id);
    return tv;
}

    /** on start_req */
void DepGraph::start_pie(PieInfo &pi, 
                         Vertex<PieInfo> **pv, 
                         Vertex<TxnInfo> **tv
                         /*, std::unordered_map<cell_locator_t, 
                           int, cell_locator_t_hash> &opset,
                           cell_entry_map_t &entry_map*/) {
    verify(tv != NULL);
    if (pv) {
        foi_pie(pi, pv);
    }

    if (tv) {
        foi_txn(pi.txn_id_, tv);
    }
  
    
    static auto id = Config::get_config()->get_site_id();
    (*tv)->data_.servers_.insert(id);
}

//void DepGraph::conflict_txn(std::unordered_map<cell_locator_t, int, cell_locator_t_hash> &opset,
//        std::vector<TxnInfo*> &conflict_txns,
//        cell_entry_map_t &entry_map) {
//    for (auto& kv: opset) {
//        auto &locator = kv.first;
//
//        entry_t *entry;
//        cell_entry_map_t::iterator entry_it = entry_map.find(locator);
//        if (entry_it == entry_map.end()) {
//            //std::map<int, entry_t *>::iterator s_entry_it = rw_entries_[locator.tbl_name][locator.primary_key].find(locator.col_id);
//            //verify(s_entry_it != rw_entries_[locator.tbl_name][locator.primary_key].end());
//            entry = rw_entries_[locator.tbl_name][locator.primary_key][locator.col_id];
//        } else {
//            entry = entry_it->second;
//        }
//
//        auto &wst = entry->wst;
//
//        if (wst != nullptr) {
//            conflict_txns.push_back(&wst->data_);
//        }
//
//        //std::list<Vertex<PieInfo>*> &list = entries_[kv.first];
//        //Log::debug("looking for parent for a new piece, %d in the list", list.size());
//        //
//        //auto it = list.end();
//
//        //while (it-- != list.begin()) {
//        //    Vertex<PieInfo> *source = *it;
//        //    int source_type = source->data_.opset_[kv.first];
//
//        //    if (source_type & OP_W) {
//        //        Vertex<TxnInfo> *st = txn_gra_.find(source->data_.txn_id_);
//        //        verify (st != NULL);
//        //        conflict_txns.push_back(&st->data_);
//        //        break;
//        //    } else {
//        //        continue;
//        //    }
//        //}
//    }
//
//}


uint64_t DepGraph::sub_txn_graph(uint64_t tid, GraphMarshaler &gra_m) {
    gra_m.gra = &txn_gra_;

    Vertex<TxnInfo> *source = txn_gra_.find(tid);
    verify(source != NULL);
    //Log::debug("compute for sub graph, tid: %llx parent size: %d", tid, (int) source->from_.size());

    auto &ret_set = gra_m.ret_set;

    find_txn_anc_opt(source, ret_set);
    ret_set.insert(source);
    return ret_set.size();
}

void DepGraph::find_txn_anc_opt(Vertex<TxnInfo> *source,
        std::unordered_set<Vertex<TxnInfo>*> &ret_set) {
    verify(source != NULL);
    //Log::debug("compute for sub graph, tid: %llx parent size: %d", tid, (int) source->from_.size());

    std::vector<Vertex<TxnInfo>*> search_stack;
    search_stack.push_back(source);

    while (search_stack.size() > 0) {
        Vertex<TxnInfo> *v  = search_stack.back();
        search_stack.pop_back();

        for (auto &kv: v->from_) {
            auto &parent = kv.first;
            if (! parent->data_.is_commit() && ret_set.find(parent) == ret_set.end()) {
                ret_set.insert(parent);
                search_stack.push_back(parent);
            }
        }

        //if (ret_set.find(v) == ret_set.end() ) {
        //        //ret_set.insert(v); // FIXME I am not sure!
        //    if (!v->data_.is_commit()) {
        //        ret_set.insert(v); // FIXME I am not sure!
        //        for (auto &kv: v->from_) {
        //            if (ret_set.find(kv.first) == ret_set.end()) {
        //                search_stack.push_back(kv.first);
        //            }
        //        }
        //    } else {
        //    }
        //} else {
        //    // this one I have gone through.
        //}
    }
    // remove source from result set.
    ret_set.erase(source);
    //if (RandomGenerator::rand(1, 100) == 1) {
    //    Log::info("anc size: %d", ret_set.size());
    //}
}

void DepGraph::find_txn_anc_opt(uint64_t txn_id,
        std::unordered_set<Vertex<TxnInfo>*> &ret_set) {
    Vertex<TxnInfo> *source = txn_gra_.find(txn_id);
    verify(source != NULL);
    find_txn_anc_opt(source, ret_set);
}

void DepGraph::find_txn_scc_anc_opt(uint64_t txn_id,
        std::unordered_set<Vertex<TxnInfo>*> &ret_set) {
    std::vector<Vertex<TxnInfo>*> scc = txn_gra_.find_scc(txn_id);
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
