#pragma once

#include "deptran/classic/scheduler.h"

namespace janus {

class NoneSched: public Scheduler {
 using Scheduler::Scheduler;
 public:
  virtual bool HandleConflicts(TxBox& dtxn,
                               innid_t inn_id,
                               vector<string>& conflicts) {
    // do nothing for none.
    return false;
  };

};

} // janus