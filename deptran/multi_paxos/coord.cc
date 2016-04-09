
#include "../__dep__.h"
#include "../constants.h"
#include "coord.h"
#include "commo.h"

namespace rococo {


MultiPaxosCoord::MultiPaxosCoord(uint32_t coo_id,
                                 int32_t benchmark,
                                 ClientControlServiceImpl *ccsi,
                                 uint32_t thread_id)
    : Coordinator(coo_id, benchmark, ccsi, thread_id) {
}


void MultiPaxosCoord::Submit(ContainerCommand& cmd,
                             const function<void()>& func,
                             const std::function<void()>& exe_callback) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  verify(!in_submission_);
  verify(cmd_ == nullptr);
  verify(cmd.self_cmd_ != nullptr);
  in_submission_ = true;
  cmd_ = &cmd;
  commit_callback_ = func;
  Prepare();
}

ballot_t MultiPaxosCoord::PickBallot() {
  // TODO
  return 1;
}

void MultiPaxosCoord::Prepare() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  verify(!in_prepare_);
  in_prepare_ = true;
  curr_ballot_ = PickBallot();
  verify(slot_id_ > 0);
  Log_debug("multi-paxos coordinator broadcasts prepare, "
                "par_id_: %lx, slot_id: %llx",
            par_id_,
            slot_id_);
  verify(n_prepare_ack_ == 0);
  commo()->BroadcastPrepare(par_id_,
                            slot_id_,
                            curr_ballot_,
                            std::bind(&MultiPaxosCoord::PrepareAck,
                                      this,
                                      phase_,
                                      std::placeholders::_1));
}

void MultiPaxosCoord::PrepareAck(phase_t phase, Future *fu) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  if (phase_ != phase) return;
  ballot_t max_ballot;
  fu->get_reply() >> max_ballot;
  if (max_ballot == curr_ballot_) {
    n_prepare_ack_++;
  } else {
    verify(0);
  }
  verify(n_prepare_ack_ <= n_replica_);
  if (n_prepare_ack_ >= GetQuorum()) {
    GotoNextPhase();
  } else {
    // TODO what if receive majority of rejects.
    // Do nothing.
  }
}

void MultiPaxosCoord::Accept() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  verify(!in_accept);
  in_accept = true;
  Log_debug("multi-paxos coordinator broadcasts accept, "
                "par_id_: %lx, slot_id: %llx",
            par_id_, slot_id_);
  auto cmd = (ContainerCommand*) cmd_;
  commo()->BroadcastAccept(par_id_,
                          slot_id_,
                          curr_ballot_,
                          *cmd,
                          std::bind(&MultiPaxosCoord::AcceptAck,
                                    this,
                                    phase_,
                                    std::placeholders::_1));
}

void MultiPaxosCoord::AcceptAck(phase_t phase, Future *fu) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  if (phase_ > phase) return;
  TxnCommand *cmd = (TxnCommand *) cmd_;
  ballot_t max_ballot;
  fu->get_reply() >> max_ballot;
  if (max_ballot == curr_ballot_) {
    n_finish_ack_++;
  } else {
    verify(0);
  }
  if (n_finish_ack_ >= GetQuorum()) {
    committed_ = true;
    GotoNextPhase();
  } else {
    // TODO
    // Do nothing.
  }
}

void MultiPaxosCoord::Commit() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  commit_callback_();
  auto cmd = (ContainerCommand*) cmd_;
  commo()->BroadcastDecide(par_id_, slot_id_, curr_ballot_, *cmd);
  verify(phase_ == Phase::COMMIT);
  GotoNextPhase();
}


void MultiPaxosCoord::GotoNextPhase() {
  int n_phase = 4;
  int current_phase = phase_ % n_phase;
  switch (phase_++ % n_phase) {
    case Phase::INIT_END:
      if (IsLeader()) {
        phase_++;
        Accept();
      } else {
        Prepare();
      }
//      verify(phase_ % n_phase_ == Phase::DISPATCH);
      break;
    case Phase::ACCEPT:
      verify(phase_ % n_phase == Phase::COMMIT);
      if (committed_) {
        Commit();
      } else {
        verify(0);
      }
      break;
    case Phase::PREPARE:
      verify(phase_ % n_phase == Phase::ACCEPT);
      Accept();
      break;
    case Phase::COMMIT:
      // do nothing.
      break;
    default:
      verify(0);
  }
}

} // namespace rococo