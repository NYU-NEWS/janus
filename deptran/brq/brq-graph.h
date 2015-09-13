
#pragma once

#include "all.h"

namespace rococo {

class BRQDTxn;

class BRQGraph {
public:
  std::map<txnid_t, BRQDTxn*> index_;

  void insert(BRQDTxn *dtxn);
  BRQDTxn* search(txnid_t txn_id);

};
} // namespace rococo
