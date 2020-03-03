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

class PaxosServer : public TxLogServer {
 public:
  // ----min_active <= max_executed <= max_committed---
  slotid_t min_active_slot_ = 0; // anything before (lt) this slot is freed
  slotid_t max_executed_slot_ = 0;
  slotid_t max_committed_slot_ = 0;
  map<slotid_t, shared_ptr<PaxosData>> logs_{};
  int n_prepare_ = 0;
  int n_accept_ = 0;
  int n_commit_ = 0;
  bool in_applying_logs_{false};

  ~PaxosServer() {
    Log_info("site par %d, loc %d: prepare %d, accept %d, commit %d", partition_id_, loc_id_, n_prepare_, n_accept_, n_commit_);
  }

  shared_ptr<PaxosData> GetInstance(slotid_t id) {
    verify(id >= min_active_slot_);
    auto& sp_instance = logs_[id];
    if(!sp_instance)
      sp_instance = std::make_shared<PaxosData>();
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
