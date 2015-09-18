
#pragma once

#include <map>

#include "all.h"

namespace rococo {
class BRQDTxn;
class CommitReply;

class BRQVertex {
public:
  std::map<BRQVertex *, int> from_; 
  std::map<BRQVertex *, int> to_; 
  std::shared_ptr<BRQDTxn> dtxn_;
};

class BRQGraph {
public:
  enum subgraph_t {
    OPT,
    SCC
  };
  std::map<txnid_t, BRQVertex *> vertex_index_;

  // insert a new txn
  void Insert(BRQDTxn *dtxn);
  // search by id
  BRQDTxn* Find(txnid_t txn_id);
  BRQDTxn* Create(txnid_t txn_id);
  BRQDTxn* FindOrCreate(txnid_t txn_id);
  // take a union of the incoming graph
  void TestExecute(BRQVertex* dtxn);
  void Aggregate(BRQGraph &subgraph);
  BRQVertex* AggregateVertex(BRQVertex*);
  void BuildEdgePointer(BRQGraph&, std::map<txnid_t, BRQVertex*>&);
  void CheckStatusChange(std::map<txnid_t, BRQVertex*>& dtxn_map);
  bool CheckPredCMT(BRQDTxn*);
  bool CheckPredFIN(std::set<BRQVertex*>& scc);
  std::set<BRQVertex*> FindSCC(BRQVertex*);   

  // this transaction waits until can be executed.
  // void WaitDCD(BRQDTxn *dtxn);
  // void WaitCMT(BRQDTxn *dtxn);
  BRQGraph(){}
  BRQGraph(const BRQGraph&) = delete;
};
} // namespace rococo
