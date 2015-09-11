
#pragma once

#include "all.h"

namespace rococo {

class BRQGraph {
public:
  std::map<txnid_t, BRQDTxn*> index_;

  void insert(BRQDTxn *dtxn) {
    // add into index
    auto ret = index_.insert(std::pair<txnid_t, BRQDTxn*>(txn_id, dtxn));
    if (ret.second == false) {
      verify(0);
    }
  }

  BRQDtxn* search(txnid_t txn_id) {
    auto it = index_.find(txn_id);
    if (it != index_.end()) {
      return it->second;
    } else {
      return nullptr;
    }
  }

};
} // namespace rococo
