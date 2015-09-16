
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
  std::map<txnid_t, BRQDTxn *> index_;

  // insert a new txn
  void     insert(BRQDTxn *dtxn);

  // search by id
  BRQDTxn* search(txnid_t txn_id);

  // take a union of the incoming graph
  void     aggregate(BRQSubGraph *subgraph);

  // this transaction waits until can be executed.
  void     wait_decided(BRQDTxn                      *dtxn,
                        std::function<void()>function) { // TODO
  }
};
} // namespace rococo
