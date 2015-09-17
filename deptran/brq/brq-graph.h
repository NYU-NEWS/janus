
#pragma once

#include <map>

#include "all.h"

namespace rococo {
class BRQDTxn;
class BRQSubGraph;
class CommitReply;

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
  BRQDTxn* Search(txnid_t txn_id);

  // take a union of the incoming graph
  void Aggregate(BRQSubGraph *subgraph);

  // this transaction waits until can be executed.
  void WaitDCD(BRQDTxn *dtxn);

  void WaitCMT(BRQDTxn *dtxn);
};
} // namespace rococo
