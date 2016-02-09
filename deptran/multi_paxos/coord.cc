
#include "../__dep__.h"
#include "../constants.h"
#include "coord.h"
#include "commo.h"

namespace rococo {

ballot_t MultiPaxosCoord::next_slot_s = 1;

MultiPaxosCoord::MultiPaxosCoord(uint32_t coo_id,
                                 int32_t benchmark,
                                 ClientControlServiceImpl *ccsi,
                                 uint32_t thread_id,
                                 bool batch_optimal)
    : Coordinator(coo_id, benchmark, ccsi, thread_id, batch_optimal) {
  slot_id_ = next_slot_s++;
}


void MultiPaxosCoord::Submit(SimpleCommand& cmd,
                             const function<void()>& func) {
  verify(cmd_ == nullptr);
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
  TxnCommand* cmd = (TxnCommand*) cmd_;
  curr_ballot_ = PickBallot();
  verify(slot_id_ > 0);
  commo()->BroadcastPrepare(par_id_,
                            slot_id_,
                            curr_ballot_,
                            std::bind(&MultiPaxosCoord::PrepareAck,
                                      this,
                                      phase_,
                                      std::placeholders::_1));
}

void MultiPaxosCoord::PrepareAck(phase_t phase, Future *fu) {
  TxnCommand* cmd = (TxnCommand*) cmd_;
  n_prepare_ack_ ++;
  ballot_t max_ballot;
  fu->get_reply() >> max_ballot;
  if (max_ballot == curr_ballot_) {
    n_prepare_ack_++;
  } else {
    verify(0);
  }
  if (n_prepare_ack_ >= GetQuorum()) {
    Accept();
  } else {
    // TODO what if receive majority of rejects.
    // Do nothing.
  }
}

void MultiPaxosCoord::Accept() {
  phase_++;
  TxnCommand* cmd = (TxnCommand*) cmd_;
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