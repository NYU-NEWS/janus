#pragma once

#include "../rcc/dep_graph.h"

namespace rococo {
class JanusDTxn;
class CommitReply;

typedef Vertex<TxnInfo> JanusVertex;
typedef vector<JanusVertex*> JanusScc;

class JanusGraph : public RccGraph {
public:
//  enum subgraph_t {
//    OPT,
//    SCC
//  };

  // take a union of the incoming graph
//  void TestExecute(BRQVertex* dtxn);
//  void Aggregate(JanusGraph &subgraph);
//  BRQVertex* AggregateVertex(BRQVertex*);
//  void BuildEdgePointer(JanusGraph&, std::map<txnid_t, BRQVertex*>&);
//  void CheckStatusChange(std::map<txnid_t, BRQVertex*>& dtxn_map);
//  bool CheckPredCMT(BRQVertex*);
//  bool CheckPredFIN(VertexList& scc);

  // this transaction waits until can be executed.
  // void WaitDCD(BRQDTxn *dtxn);
  // void WaitCMT(BRQDTxn *dtxn);
  using RccGraph::RccGraph;
//  JanusGraph(const JanusGraph&) = delete;
};
} // namespace rococo
