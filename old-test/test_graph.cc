
#include "deptran/all.h"

namespace rococo {

//class IntV {
//public:
//    uint64_t i;
//    
//    IntV(): i(0) {
//        ;
//    }
//
//    IntV(uint64_t j): i(j) {
//        ;
//    }
//
//    inline uint64_t id() {
//        return i;
//    }
//    
//    inline bool operator <(const IntV& j) const {
//        return i < j.i;
//    }
//};
//
///** only valid for int */
//inline std::ostream& operator<< (std::ostream &out, Graph<IntV>& gra) {
//    for (auto& v: gra.vertex_index_) {
//        out << v->data_.id() <<" ";
//        for (auto& kv: v->to_) {
//            out << kv.first->data_.id()<< " ";
//        }
//        out << "#";
//    }
//    out << std::endl;
//    return out;
//}
//
//inline std::istream& operator>> (std::istream &in, Graph<IntV>& gra) {
//    char c;
//    while (in) {
//        IntV s; 
//        in >> s.i;
//        Vertex<IntV> *v = gra.find_or_insert(s);
//        while (in >> c) {
//            if (c == ' ') {
//                continue;
//            } else if (c == '#') {
//                break;
//            } else {
//                IntV t;
//                t.i = c - '0';
//                Vertex<IntV> *vt = gra.find_or_insert(t);
//                v->to_[vt] = 1;
//                vt->from_[v] = 1;
//            }
//        }
//    }
//    return in;
//}
//
//TEST(graph, simple) {
//
//    Graph<TxnInfo> gra;
//
//    Vertex<TxnInfo> v1;
//    v1.data_.txn_id_ = 1;
//
//    Vertex<TxnInfo> v2;
//    v2.data_.txn_id_ = 2;
//
//    v2.to_[&v1] = 1;
//
//    EXPECT_EQ(gra.insert(&v1), true);
//    EXPECT_EQ(gra.insert(&v2), true);
//    EXPECT_EQ(v1.from_.size(), 1);
//
//    EXPECT_EQ(gra.remove(&v2), true);
//    EXPECT_EQ(v1.from_.size(), 0);
//
//}
//
//TEST(graph, simple_2) {
//    Graph<IntV> gra1;
//    Vertex<IntV> v1(1);
//    Vertex<IntV> v2(2);
//    v2.to_[&v1] = 1;
//    
//    EXPECT_EQ(gra1.insert(&v1), true);
//    EXPECT_EQ(gra1.insert(&v2), true);
//    EXPECT_EQ(v1.from_.size(), 1);
//    std::cout << gra1;
//
//    EXPECT_EQ(gra1.remove(&v2), true);
//    EXPECT_EQ(v1.from_.size(), 0);
//    std::cout << gra1;
//
//    Graph<IntV> gra2; 
//    
//    std::istringstream iss("1 2 3# 2 3# 3#");
//    iss >> gra2;
//    std::cout << gra2;
//}
//
//TEST(graph, scc) {
//    Graph<IntV> gra1;
//    std::istringstream iss("1 2# 2 1 5# 3 4# 4 3 5# 5 6# 6 7# 7 8# 8 6 9# 9#");
//    iss >> gra1;
//    std::function<void(int)> func = [&gra1] (int i) {
//        IntV j(i);
//        std::vector<Vertex<IntV>*> v1 = gra1.find_scc(j); 
//        for (auto v: v1) {
//            std::cout << v->data_.id() << " ";
//        } 
//        std::cout<<std::endl;
//    };
//
//    for (int i = 1; i <= 9; i++) {
//        func(i);
//    }
//}
//
//TEST(graph, union_op) {
//    Graph<TxnInfo> gra1;
//    Graph<TxnInfo> gra2;
//
//    Vertex<TxnInfo> v1;
//    v1.data_.txn_id_ = 1;
//
//    Vertex<TxnInfo> v2;
//    v2.data_.txn_id_ = 2;
//
//    Vertex<TxnInfo> v2p;
//    v2p.data_.txn_id_ = 2;
//
//    Vertex<TxnInfo> v3;
//    v3.data_.txn_id_ = 3;
//
//    gra1.insert(&v1);
//    v2.from_[&v1] = 1;
//    gra1.insert(&v2);
//
//
//    gra2.insert(&v2p);
//    v3.from_[&v2p] = 1;
//    gra2.insert(&v3);
//
//    gra1.union_graph(gra2);
//    EXPECT_EQ(gra1.size(), 3);
//    EXPECT_EQ(v2.to_.size(), 1);
//
//}
//


} // namespace deptran
