#pragma once

#include "../__dep__.h"
#include "../coordinator.h"
#include "../frame.h"

namespace janus {

class MultiPaxosCommo;
class CoordinatorMultiPaxos : public Coordinator {
 public:
//  static ballot_t next_slot_s;
 private:
  enum Phase { INIT_END = 0, PREPARE = 1, ACCEPT = 2, COMMIT = 3 };
  const int32_t n_phase_ = 4;

  MultiPaxosCommo *commo() {
    // TODO fix this.
    verify(commo_ != nullptr);
    return (MultiPaxosCommo *) commo_;
  }
  bool in_submission_ = false; // debug;
  bool in_prepare_ = false; // debug
  bool in_accept = false; // debug
 public:
  shared_ptr<Marshallable> cmd_{nullptr};
  CoordinatorMultiPaxos(uint32_t coo_id,
                        int32_t benchmark,
                        ClientControlServiceImpl *ccsi,
                        uint32_t thread_id);
  ballot_t curr_ballot_ = 1; // TODO
  uint32_t n_replica_ = 0;   // TODO
  slotid_t slot_id_ = 0;
  slotid_t *slot_hint_ = nullptr;

  uint32_t n_replica() {
    verify(n_replica_ > 0);
    return n_replica_;
  }

  bool IsLeader() {
    return this->loc_id_ == 0;
  }

  slotid_t GetNextSlot() {
    verify(0);
    verify(slot_hint_ != nullptr);
    slot_id_ = (*slot_hint_)++;
    return 0;
  }

  uint32_t GetQuorum() {
    return n_replica() / 2 + 1;
  }

  void DoTxAsync(TxRequest &req) override {}
  void Submit(shared_ptr<Marshallable> &cmd,
              const std::function<void()> &func = []() {},
              const std::function<void()> &exe_callback = []() {}) override;

  ballot_t PickBallot();
  void Prepare();
//  void PrepareAck(phase_t phase, Future *fu);
  void Accept();
//  void AcceptAck(phase_t phase, Future *fu);
  void Commit();

  void Reset() override {}
  void Restart() override { verify(0); }

  void GotoNextPhase();
};

} //namespace janus
