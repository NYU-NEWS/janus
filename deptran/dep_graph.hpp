#pragma once

#include "all.h"

/**
 * This is NOT thread safe!!!
 */
namespace rococo {

class DepGraph {
public:
//    Graph<PieInfo> pie_gra_;
    Graph<TxnInfo> txn_gra_;

    /** the entry to (vertexes, type) (a column)*/
    //std::unordered_map<
    //    cell_locator,
    //    entry_t,
    //    cell_locator_hasher>
    //        rw_entries_;
    //
    //std::map<char *, std::unordered_map<mdb::MultiBlob, std::map<int, entry_t *>, mdb::MultiBlob::hash>> rw_entries_;

    //std::unordered_map<cell_locator, std::list<Vertex<PieInfo>*>, cell_locator_hasher> entries_;

    std::vector<rrr::Client*> rpc_clients_;
    std::vector<RococoProxy*> rpc_proxies_;
    std::vector<std::string> server_addrs_;


    DepGraph() {
        Config::get_config()->get_all_site_addr(server_addrs_);
        rpc_clients_ = std::vector<rrr::Client*>(server_addrs_.size(), nullptr);
        rpc_proxies_ = std::vector<RococoProxy*>(server_addrs_.size(), nullptr);
//      rw_entries_ = std::unordered_map<cell_locator, entry_t, 
//        cell_locator_hasher>(10000);
    }

    ~DepGraph() {// XXX some memory leak does not hurt.
    }

    RococoProxy* get_server_proxy(uint32_t id);


    void foi_pie_txn(PieInfo &pi, Vertex<PieInfo> **pv, Vertex<TxnInfo> **tv);

    void foi_pie(PieInfo &pi, Vertex<PieInfo> **pv);

    void foi_txn(uint64_t &txn_id, Vertex<TxnInfo> **tv);

    Vertex<TxnInfo> *start_pie_txn(uint64_t tid);

    /** on start_req */
    void start_pie(
            txnid_t txn_id,
            Vertex<TxnInfo> **tv
    );

    void union_txn_graph(Graph<TxnInfo>& gra) {
        txn_gra_.union_graph(gra, true);
    }

    std::vector<Vertex<TxnInfo>*> find_txn_scc(TxnInfo &ti) {
        return txn_gra_.find_scc(ti);
    }

    void find_txn_anc_opt(Vertex<TxnInfo>* source,
            std::unordered_set<Vertex<TxnInfo>*> &ret_set);

    void find_txn_anc_opt(uint64_t txn_id,
            std::unordered_set<Vertex<TxnInfo>*> &ret_set);

    void find_txn_nearest_anc(Vertex<TxnInfo>* v,
            std::set<Vertex<TxnInfo>*> &ret_set) {
        for (auto &kv: v->from_) {
            ret_set.insert(kv.first);
        }
    }

    void find_txn_scc_nearest_anc(Vertex<TxnInfo> *v, 
                                  std::set<Vertex<TxnInfo>*> &ret_set) {
        std::vector<Vertex<TxnInfo>*> scc = txn_gra_.find_scc(v);
        for (auto v: scc) {
            find_txn_nearest_anc(v, ret_set);
        }

        for (auto &scc_v: scc) {
            ret_set.erase(scc_v);
        }

        //if (RandomGenerator::rand(1, 100) == 1) {
        //    Log::info("scc anc size: %d", ret_set.size());
        //}
    }


    void find_txn_scc_anc_opt(uint64_t txn_id,
            std::unordered_set<Vertex<TxnInfo>*> &ret_set);

//    void conflict_txn(std::unordered_map<cell_locator_t, int, cell_locator_t_hash> &opset,
//            std::vector<TxnInfo*> &conflict_txns,
//            cell_entry_map_t &entry_map);

    uint64_t sub_txn_graph(uint64_t tid, GraphMarshaler &gra_m);

    //void sub_txn_graph(uint64_t tid, Graph<TxnInfo> &gra) {
    //    Vertex<TxnInfo> *source = txn_gra_.find(tid);
    //    verify(source != NULL);
    //    Log::debug("compute for sub graph, tid: %llx parent size: %d", tid, (int) source->from_.size());

    //    std::unordered_set<Vertex<TxnInfo>*> ret_set;

    //    find_txn_anc_opt(tid, ret_set);
    //    ret_set.insert(source);

    //    std::map<Vertex<TxnInfo>*, Vertex<TxnInfo>*> old_new_index;

    //    for (auto &old_v: ret_set) {
    //        Vertex<TxnInfo> *new_v = new Vertex<TxnInfo>(old_v->data_);
    //        old_new_index[old_v] = new_v;
    //        gra.insert(new_v);
    //    }
    //
    //    for (auto &old_sv: ret_set) {
    //        auto new_sv = old_new_index[old_sv];
    //        for (auto &kv: old_sv->to_) {
    //            auto old_tv = kv.first;
    //            auto relation = kv.second;
    //            if (ret_set.find(old_tv) != ret_set.end()) {
    //                auto new_tv = old_new_index[old_tv];
    //                new_sv->to_[new_tv] = relation;
    //                new_tv->from_[new_sv] = relation;
    //            } else {
    //                Log::debug("this vertex is not what I want to include");
    //            }
    //        }
    //    }
    //
    //    //int anc_size = gra.find_ancestor(source).size();
    //    Log::debug("sub graph, return size: %d",  (int)gra.size());

    //    verify(gra.size() > 0);
    //
    //    // verify this graph size is ok.
    //    for (auto &v: gra.vertex_index_) {
    //        for (auto &kv: v->to_) {
    //            auto v_to = kv.first;
    //            verify(gra.find(v_to->data_.id()) != nullptr);
    //        }
    //        for (auto &kv: v->from_) {
    //            auto v_from = kv.first;
    //            verify(gra.find(v_from->data_.id()) != nullptr);
    //        }
    //    }
    //}
    //void marshal_after_start(rrr::Marshal& m, TxnInfo &ti);
    //void marshal_after_finish(rrr::Marshal& m, TxnInfo &ti);
};
} // namespace rcc
