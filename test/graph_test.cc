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

  // TODO: should a graph vertex object really need these?
  void trigger() {}
  void union_data(const Id& ti, bool trigger = true, bool server = false) {}
};

Graph<Id>* CompleteGraph(const int n, int id_start = 0) {
  Graph<Id>* graph = new Graph<Id>();
  std::map<int, Vertex<Id>*> nodes;
  for (int i = id_start; i < id_start + n; i++) {
    nodes[i] = graph->CreateV(i);
  }
  for (int i = id_start; i < id_start + n; i++) {
    for (int j = id_start; j < id_start + n; j++) {
      if (i == j) continue;
      nodes[i]->AddEdge(nodes[j], 0);
    }
  }
  return graph;
}

Graph<Id>* ParseGraph(const char* graph_str) {
  Graph<Id>* graph = new Graph<Id>();
  std::map<int, Vertex<Id>*> nodes;
  std::vector<int> endpoints;
  for (size_t i = 0; i < strlen(graph_str); i++) {
    switch (graph_str[i]) {
      case ' ':
        break;
      case '#':
        if (endpoints.size() == 2) {
          auto from = nodes[endpoints[0]];
          auto to = nodes[endpoints[1]];
          from->AddEdge(to, 0);
          endpoints.clear();
        } else {
          printf("bad format: ");
          for (int i : endpoints) printf("%d ", i);
          printf("\n");
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
  v2->AddEdge(v1, 0);
  ASSERT_EQ(1, v1->incoming_.size());
  ASSERT_EQ(1, v2->outgoing_.size());
}

TEST(GraphTest, SccWorksForSmallGraph) {
  auto g = ParseGraph("11 22# 22 11# 33 44# 44 33# 55 66#");
  std::vector<Vertex<Id>*> scc;

  int scc2_ids[] = {11, 22, 33, 44};
  for (int i : scc2_ids) {
    scc = g->FindSCC(i);
    ASSERT_EQ(2, scc.size());
  }

  int scc1_ids[] = {55, 66};
  for (int i : scc1_ids) {
    scc = g->FindSCC(i);
    ASSERT_EQ(1, scc.size());
    ASSERT_EQ(i, scc[0]->id());
  }
  delete g;
}

TEST(GraphTest, SccWorksForCompleteGraph) {
  const int num = 100;
  auto g = CompleteGraph(num);
  for (int i = 0; i < num; i++) {
    ASSERT_EQ(num, g->FindSCC(i).size());
  }
  delete g;
}

TEST(GraphTest, SccWorksForSimpleCycle) {
  const int num = 100;
  Graph<Id> graph;
  std::vector<Vertex<Id>*> nodes;
  for (int i = 0; i < num; i++) {
    nodes.push_back(graph.CreateV(i));
  }
  for (int i = 0; i < num; i++) {
    if (i != num - 1) {
      nodes[i]->AddEdge(nodes[i + 1], 0);
    } else {
      nodes[i]->AddEdge(nodes[0], 0);
    }
  }
  for (int i = 0; i < num; i++) {
    ASSERT_EQ(num, graph.FindSCC(i).size());
  }
}

TEST(GraphTest, SccWorksForMultipleComponents) {
  const int num = 100;
  std::vector<Graph<Id>*> components;
  for (int i = 0; i < num; i++) {
    components.push_back(CompleteGraph(num, i * num));
  }
  for (int i = 1; i < num; i++) {
    components[0]->Aggregate(*components[i], false);
  }
  auto scc_results = components[0]->SCC();
  ASSERT_EQ(num, scc_results.size());
  ASSERT_EQ(num * num, components[0]->size());
  for (int i = 0; i < num; i++) {
    ASSERT_EQ(num, scc_results[i].size());
  }
}

TEST(GraphTest, CanAggregateGraphs) {
  auto g1 = ParseGraph("11 22# 22 11#");
  auto g2 = ParseGraph("33 44# 44 33#");
  g1->Aggregate(*g2, false);
  ASSERT_EQ(33, g1->FindV(33)->id());
  ASSERT_EQ(44, g1->FindV(44)->id());
}
