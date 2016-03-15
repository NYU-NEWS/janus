
#include "graph.h"
#include "graph_marshaler.h"


namespace rococo {
//
//void GraphMarshaler::marshal_help_1(
//        rrr::Marshal &m,
//        const std::unordered_set<Vertex<TxnInfo>*> &ret_set,
//        Vertex<TxnInfo> *old_sv
//) const {
//    int32_t to_size = 0;
//    //if (RandomGenerator::rand(1,200) == 1) {
//    //    Log::info("direct parent number, size: %d",  (int)old_sv->to_.size());
//    //}
//    std::vector<Vertex<TxnInfo>*> to;
//    std::vector<int8_t> relation;
//    for (auto &kv: old_sv->outgoing_) {
//        auto old_tv = kv.first;
//        if (ret_set.find(old_tv) != ret_set.end()) {
//            to_size++;
//            to.push_back(kv.first);
//            relation.push_back(kv.second);
//        } else {
//            //Log::debug("this vertex is not what I want to include");
//        }
//    }
//    m << to_size;
//    for (int i = 0; i < to_size; i++) {
//        uint64_t id = to[i]->data_->id();
//        m << id;
//        m << relation[i];
//    }
//}
//
//void GraphMarshaler::marshal_help_2(
//        rrr::Marshal &m,
//        const std::unordered_set<Vertex<TxnInfo>*> &ret_set,
//        Vertex<TxnInfo> *old_sv
//) const {
//        //int32_t ma_size = 0;
//        for (auto &kv: old_sv->outgoing_) {
//            auto old_tv = kv.first;
//            //int8_t relation = kv.second;
//            if (ret_set.find(old_tv) != ret_set.end()) {
//                //uint64_t id = old_tv->data_.id();
//                //m << id;
//                //m << relation;
//                //ma_size++;
//            } else {
//                //Log::debug("this vertex is not what I want to include");
//            }
//        }
//}
//
//void GraphMarshaler::write_to_marshal(rrr::Marshal &m) const {
//    const GraphMarshaler &gra_m = *this;
//    verify(gra_m.gra != nullptr);
//    //Graph<TxnInfo> &gra = *(gra_m.gra);
//    auto &ret_set = gra_m.ret_set;
//    int32_t n = ret_set.size();
//    m << n;
//
//    for (auto &old_sv: ret_set) {
//        m << old_sv->data_->id();
//        m << *(old_sv->data_);
//
//        marshal_help_1(m, ret_set, old_sv);
//
////        marshal_help_2(m, ret_set, old_sv);
//
////      verify(ma_size == to_size);
//    }
//
//    //if (RandomGenerator::rand(1,200) == 1) {
//    //    Log::info("sub graph in start reply, size: %d",  (int)n);
//    //}
//    //Log::debug("sub graph, return size: %d",  (int)n);
//}

} // namespace rococo
