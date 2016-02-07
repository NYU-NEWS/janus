#pragma once

#include "__dep__.h"
#include "constants.h"
#include "scheduler.h"

namespace rococo {

class MultiPaxosSched : public Scheduler {
 public:
  void OnPrepareRequest(slotid_t slot_id,
                       ballot_t ballot,
                       ballot_t* max_ballot,
                       const function<void()>& cb);

  void OnAcceptRequest(slotid_t slot_id,
                       ballot_t ballot,
                       ballot_t* max_ballot,
                       const function<void()>& cb);

  void OnDecideRequest(slotid_t slot_id, ballot_t ballot);
};

} // namespace rococo