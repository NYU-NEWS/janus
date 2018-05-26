#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../scheduler.h"

namespace janus {
class Command;
class CmdData;

struct PaxosData {
  ballot_t max_ballot_seen_ = 0;
  ballot_t max_ballot_accepted_ = 0;
  shared_ptr<Marshallable> accepted_cmd_{nullptr};
  shared_ptr<Marshallable> committed_cmd_{nullptr};
};

class SchedulerMultiPaxos : public Scheduler {
 public:
  slotid_t max_executed_slot_ = 0;
  slotid_t max_committed_slot_ = 0;
  map<slotid_t, shared_ptr<PaxosData>> logs_{};

  shared_ptr<PaxosData> GetInstance(slotid_t id) {
    auto& sp_instance = logs_[id];
    if(!sp_instance)
      sp_instance.reset(new PaxosData);
    return sp_instance;
  }

  void OnPrepare(slotid_t slot_id,
                 ballot_t ballot,
                 ballot_t *max_ballot,
                 const function<void()> &cb);

  void OnAccept(const slotid_t slot_id,
                const ballot_t ballot,
                shared_ptr<Marshallable> &cmd,
                ballot_t *max_ballot,
                const function<void()> &cb);

  void OnCommit(const slotid_t slot_id,
                const ballot_t ballot,
                shared_ptr<Marshallable> &cmd);

  virtual bool HandleConflicts(Tx& dtxn,
                               innid_t inn_id,
                               vector<string>& conflicts) {
    verify(0);
  };
};
} // namespace janus
