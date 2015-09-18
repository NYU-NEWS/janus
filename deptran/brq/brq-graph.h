
#pragma once

#include <map>

#include "all.h"

namespace rococo {
class BRQDTxn;
class CommitReply;

class BRQVertex {
  std::map<BRQVertex *, int> from_; 
  std::map<BRQVertex *, int> to_; 
  
  std::shared_ptr<BRQDTxn> dtxn;
};

class BRQGraph {
public:
  enum subgraph_t {
    OPT,
    SCC
  };
  std::map<txnid_t, BRQDTxn *> vertex_index_;

  // insert a new txn
  void Insert(BRQDTxn *dtxn);
  // search by id
  BRQDTxn* Find(txnid_t txn_id);
  BRQDTxn* Create(txnid_t txn_id);
  BRQDTxn* FindOrCreate(txnid_t txn_id);
  // take a union of the incoming graph
  void Aggregate(BRQGraph &subgraph);
  BRQDTxn* AggregateVertex(BRQDTxn *dtxn);
  void BuildEdgePointer(std::map<txnid_t, BRQDTxn*>&);
  void TestExecute(BRQDTxn* dtxn);
  void CheckStatusChange(std::map<txnid_t, BRQDTxn*>& dtxn_map);
  bool CheckPredCMT(BRQDTxn*);
  bool CheckPredFIN(std::set<BRQDTxn*>& scc);
  std::set<BRQDTxn*> FindSCC(BRQDTxn*);   

  // this transaction waits until can be executed.
  // void WaitDCD(BRQDTxn *dtxn);
  // void WaitCMT(BRQDTxn *dtxn);
};
} // namespace rococo
