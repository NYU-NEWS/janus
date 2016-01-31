
#include "__dep__.h"
#include "constants.h"
#include "coord.h"

namespace rococo {

void MultiPaxosCoord::Submit(SimpleCommand& cmd, std::function<void()> func) {
  cmd_ = new SimpleCommand();
  *cmd_ = cmd;
  callback_ = func;
}

ballot_t MultiPaxosCoord::PickBallot() {
  // TODO
  return 1;
}

void MultiPaxosCoord::Prepare() {
  TxnCommand* cmd = (TxnCommand*) cmd_;
  curr_ballot_ = PickBallot();
  commo_->BroadcastPrepare(par_id_,
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
  if (n_prepare_ack_ >= n_replica_ / 2 + 1) {
    Accept();
  } else {
    // TODO what if receive majority of rejects.
    // Do nothing.
  }
}

void MultiPaxosCoord::Accept() {
  phase_++;
  TxnCommand* cmd = (TxnCommand*) cmd_;
  commo_->BroadcastAccept(par_id_,
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
  if (n_finish_ack_ >= n_replica_ / 2 + 1) {
    Decide();
  } else {
    // TODO
    // Do nothing.
  }
}

void MultiPaxosCoord::Decide() {
  phase_++;
  auto cmd = (TxnCommand*) cmd_;
  commo_->BroadcastDecide(par_id_, curr_ballot_, *cmd);
}

} // namespace rococo