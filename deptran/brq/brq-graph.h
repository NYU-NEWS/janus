
#pragma once

#include <map>

#include "all.h"

namespace rococo {

class BRQDTxn;
class BRQSubGraph;

class BRQGraph {
public:
  enum subgraph_t {
    OPT,
    SCC
  };
  std::map<txnid_t, BRQDTxn*> index_;

  void insert(BRQDTxn *dtxn);
  BRQDTxn* search(txnid_t txn_id);
  void aggregate(BRQSubGraph* subgraph);
};
} // namespace rococo
