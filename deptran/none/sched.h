#pragma once

#include "../classic/sched.h"

namespace rococo {

class NoneSched: public Scheduler {
 using Scheduler::Scheduler;
 public:
  virtual bool HandleConflicts(DTxn& dtxn,
                               innid_t inn_id,
                               vector<string>& conflicts) {
    // do nothing for none.
    return false;
  };

};

}