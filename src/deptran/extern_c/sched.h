#pragma once

#include "../scheduler.h"

//
// Created by Shuai on 6/25/17.
//

namespace janus {

class ExternCScheduler : public TxLogServer {
  using TxLogServer::TxLogServer;
 public:
  virtual bool HandleConflicts(Tx& dtxn,
                               innid_t inn_id,
                               vector<string>& conflicts) override {
    verify(0);
    return false;
  };

};

} // namespace janus
