#include "gtest/gtest.h"
#include "deptran/all.h"
#include <cctype>

using namespace std;
using namespace rococo;

struct Id {
	int id_;
	Id() : id_(-1) {}
	Id(int id) : id_(id) {}
	int id() { return id_; }
};

Graph<Id>* parse_graph(const char* graph_str) {
	Graph<Id>* graph = new Graph<Id>();
	std::map<int, Vertex<Id>*> nodes;
	std::vector<int> endpoints;
	for (size_t i=0; i<strlen(graph_str); i++) {
		switch (graph_str[i]) {
			case ' ':
			case '\0':
				break;
			case '#':
				if (endpoints.size() == 2) {
					auto from = nodes[endpoints[0]];
					auto to = nodes[endpoints[1]];
					from->AddEdge(to);
				}
				break;
			default: {
				std::string buf;
				while (isdigit(graph_str[i])) {
					buf.push_back(graph_str[i]);
					i++;
				}
				i--;
				int name = atoi(buf.c_str());
				nodes[name] = graph->FindOrCreateV(name);
				endpoints.push_back(atoi(buf.c_str()));
			}
		}
	}
	return graph;
}

TEST(GraphTest, CanAddAnEdge) {
    Graph<Id> g;
	auto v1 = g.CreateV(1);
    auto v2 = g.CreateV(2);
	v2->AddEdge(v1);
    ASSERT_EQ(1, v1->incoming_.size());
    ASSERT_EQ(1, v2->outgoing_.size());
}



TEST(GraphTest, scc) {
    Graph<Id> gra1;
    parse_graph("11 22#");
}

//TEST(GraphTest, union_op) {
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
