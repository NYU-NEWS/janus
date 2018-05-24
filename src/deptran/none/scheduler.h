#pragma once

#include "deptran/classic/scheduler.h"

namespace janus {

class SchedulerNone: public Scheduler {
 using Scheduler::Scheduler;
 public:
  virtual bool HandleConflicts(Tx& dtxn,
                               innid_t inn_id,
                               vector<string>& conflicts) {
    // do nothing for none.
    return false;
  };

};

} // janus