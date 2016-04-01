
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


void MultiPaxosCoord::Submit(SimpleCommand& cmd,
                             const function<void()>& func) {
  verify(!in_submission_);
  verify(cmd_ == nullptr);
  in_submission_ = true;
  cmd_ = new SimpleCommand();
  *cmd_ = cmd;
  callback_ = func;
  Prepare();
}

ballot_t MultiPaxosCoord::PickBallot() {
  // TODO
  return 1;
}

void MultiPaxosCoord::Prepare() {
  verify(!in_prepare_);
  in_prepare_ = true;
  TxnCommand* cmd = (TxnCommand*) cmd_;
  curr_ballot_ = PickBallot();
  verify(slot_id_ > 0);
  Log_debug("multi-paxos coordinator broadcasts prepare, slot_id: %llx",
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
  if (phase_ != phase) return;
  TxnCommand* cmd = (TxnCommand*) cmd_;
  ballot_t max_ballot;
  fu->get_reply() >> max_ballot;
  if (max_ballot == curr_ballot_) {
    n_prepare_ack_++;
  } else {
    verify(0);
  }
  verify(n_prepare_ack_ <= n_replica_);
  if (n_prepare_ack_ >= GetQuorum()) {
    Accept();
  } else {
    // TODO what if receive majority of rejects.
    // Do nothing.
  }
}

void MultiPaxosCoord::Accept() {
  verify(!in_accept);
  in_accept = true;
  phase_++;
  TxnCommand* cmd = (TxnCommand*) cmd_;
  Log_debug("multi-paxos coordinator broadcasts accept, slot_id: %llx",
            slot_id_);
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
    callback_();
    Decide();
  } else {
    // TODO
    // Do nothing.
  }
}

void MultiPaxosCoord::Decide() {
  phase_++;
  auto cmd = (TxnCommand*) cmd_;
  commo()->BroadcastDecide(par_id_, slot_id_, curr_ballot_, *cmd);
}

} // namespace rococo