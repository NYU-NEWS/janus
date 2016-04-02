#pragma once

#include "__dep__.h"
#include "constants.h"
#include "scheduler.h"

namespace rococo {

class Command;
class ContainerCommand;
class MultiPaxosSched : public Scheduler {
 public:
  void OnPrepare(slotid_t slot_id,
                 ballot_t ballot,
                 ballot_t *max_ballot,
                 const function<void()> &cb);

  void OnAccept(const slotid_t slot_id,
                const ballot_t ballot,
                const ContainerCommand &cmd,
                ballot_t *max_ballot,
                const function<void()> &cb);

  void OnCommit(const slotid_t slot_id,
                const ballot_t ballot,
                const ContainerCommand &cmd);
};

} // namespace rococo