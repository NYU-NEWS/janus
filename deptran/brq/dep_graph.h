#pragma once

#include "../rcc/dep_graph.h"

namespace rococo {
class BrqDTxn;
class CommitReply;

typedef Vertex<TxnInfo> BrqVertex;
typedef vector<BrqVertex*> BrqScc;

class BrqGraph : public RccGraph {
public:
//  enum subgraph_t {
//    OPT,
//    SCC
//  };

  // take a union of the incoming graph
//  void TestExecute(BRQVertex* dtxn);
//  void Aggregate(BrqGraph &subgraph);
//  BRQVertex* AggregateVertex(BRQVertex*);
//  void BuildEdgePointer(BrqGraph&, std::map<txnid_t, BRQVertex*>&);
//  void CheckStatusChange(std::map<txnid_t, BRQVertex*>& dtxn_map);
//  bool CheckPredCMT(BRQVertex*);
//  bool CheckPredFIN(VertexList& scc);

  // this transaction waits until can be executed.
  // void WaitDCD(BRQDTxn *dtxn);
  // void WaitCMT(BRQDTxn *dtxn);
  using RccGraph::RccGraph;
//  BrqGraph(const BrqGraph&) = delete;
};
} // namespace rococo
