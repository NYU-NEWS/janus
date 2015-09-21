
#pragma once

#include <map>
//#include "graph.h"
#include "all.h"

namespace rococo {
class BRQDTxn;
class CommitReply;

typedef Vertex<BRQDTxn> BRQVertex;

class BRQGraph : public Graph<BRQDTxn> {
public:
  enum subgraph_t {
    OPT,
    SCC
  };
  std::map<txnid_t, BRQVertex *> vertex_index_;

  // take a union of the incoming graph
  void TestExecute(BRQVertex* dtxn);
  void Aggregate(BRQGraph &subgraph);
  BRQVertex* AggregateVertex(BRQVertex*);
  void BuildEdgePointer(BRQGraph&, std::map<txnid_t, BRQVertex*>&);
  void CheckStatusChange(std::map<txnid_t, BRQVertex*>& dtxn_map);
  bool CheckPredCMT(BRQVertex*);
  bool CheckPredFIN(scc_t &scc);

  // this transaction waits until can be executed.
  // void WaitDCD(BRQDTxn *dtxn);
  // void WaitCMT(BRQDTxn *dtxn);
  BRQGraph(){}
  BRQGraph(const BRQGraph&) = delete;
};
} // namespace rococo
