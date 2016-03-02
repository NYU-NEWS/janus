#include "../__dep__.h"
#include "coord.h"
#include "frame.h"
#include "commo.h"

namespace rococo {

TapirCommo* TapirCoord::commo() {
//  verify(commo_ != nullptr);
  if (commo_ == nullptr) {
    commo_ = new TapirCommo();
  }
  return (TapirCommo*)commo_;
}


void TapirCoord::Handout() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  phase_++;

  int cnt = 0;
  while (cmd_->HasMoreSubCmdReadyNotOut()) {
    auto subcmd = (SimpleCommand*) cmd_->GetNextReadySubCmd();
    subcmd->id_ = next_pie_id();
    verify(subcmd->root_id_ == cmd_->id_);
    n_handout_++;
    cnt++;
    Log_debug("send out start request %ld, cmd_id: %lx, inn_id: %d, pie_id: %lx",
              n_handout_, cmd_->id_, subcmd->inn_id_, subcmd->id_);
    handout_acks_[subcmd->inn_id()] = false;
    commo()->SendHandout(*subcmd,
                         this,
                         std::bind(&ThreePhaseCoordinator::HandoutAck,
                                   this,
                                   phase_,
                                   std::placeholders::_1,
                                   std::placeholders::_2));
  }
  Log_debug("sent %d SubCmds\n", cnt);
}


void TapirCoord::HandoutAck(phase_t phase, int res, Command& cmd) {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  verify(phase == phase_);
  n_handout_ack_++;
  TxnCommand *ch = (TxnCommand *) cmd_;
  handout_acks_[cmd.inn_id_] = true;

  Log_debug("get start ack %ld/%ld for cmd_id: %lx, inn_id: %d",
            n_handout_ack_, n_handout_, cmd_->id_, cmd.inn_id_);
  verify(res == SUCCESS);
  cmd_->Merge(cmd);
  if (cmd_->HasMoreSubCmdReadyNotOut()) {
    Log_debug("command has more sub-cmd, cmd_id: %lx,"
                  " n_started_: %d, n_pieces: %d",
              cmd_->id_, ch->n_pieces_out_, ch->GetNPieceAll());
    Handout();
  } else if (AllHandoutAckReceived()) {
    Log_debug("receive all handout acks, txn_id: %ld; START PREPARE",
              cmd_->id_);
    verify(0);
  }
}

void TapirCoord::Reset() {
  Coordinator::Reset();
  n_accept_oks_.clear();
  n_fast_accept_oks_.clear();
}

void TapirCoord::FastAccept() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  phase_++;
  for (auto par : cmd_->GetPartitionIds()) {

  }

  while (cmd_->HasMoreSubCmdReadyNotOut()) {
    auto subcmd = (SimpleCommand*) cmd_->GetNextReadySubCmd();
    subcmd->id_ = next_pie_id();
    verify(subcmd->root_id_ == cmd_->id_);
    n_handout_++;
    Log_debug("send out fast accept request %ld, cmd_id: %lx, "
                  "inn_id: %d, pie_id: %lx",
              n_handout_, cmd_->id_, subcmd->inn_id_, subcmd->id_);
    handout_acks_[subcmd->inn_id()] = false;
    commo()->BroadcastFastAccept(*subcmd,
                                 std::bind(&TapirCoord::FastAcceptAck,
                                           this,
                                           phase_,
                                           subcmd->PartitionId(),
                                           std::placeholders::_1));
  }
}

void TapirCoord::FastAcceptAck(phase_t phase, parid_t par_id, Future *fu) {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  if (phase_ > phase) return;
  // if for every partition, get fast quorum of OK, go into decide.
  // do not consider anything else for now.
  int res;
  fu->get_reply() >> res;
  verify(res == SUCCESS); // TODO
  n_fast_accept_oks_[par_id]++;
  if (FastQuorumPossible()) {
    if (AllFastQuorumReached()) {
      decision_ = COMMIT;
      Decide();
    } else {
      // wait/
    }  
  } else {
    if (AllSlowQuorumReached()) {
      decision_ = ABORT;
      Accept();
    }
  }
}

bool TapirCoord::FastQuorumPossible() {
  return true;
  verify(0);
}

void TapirCoord::Accept() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  phase_++;
  ballot_t ballot = MAGIC_SACCEPT_BALLOT;
  auto decision = ABORT;
  for (auto par_id : cmd_->GetPartitionIds()) {
    commo()->BroadcastAccept(par_id,
                             cmd_->id_,
                             ballot,
                             decision,
                             std::bind(&TapirCoord::AcceptAck,
                                       this,
                                       phase_,
                                       par_id,
                                       std::placeholders::_1));
  }
}

void TapirCoord::AcceptAck(phase_t phase, parid_t pid, Future* fu) {
  int res;
  fu->get_reply() >> res;
  verify(res == SUCCESS);
  n_accept_oks_[pid]++;
  if (n_accept_oks_[pid] >= GetSlowQuorum(pid)) {
    Decide();
  } else {
    // TODO keep waiting.
  }
}

int TapirCoord::GetFastQuorum(parid_t par_id) {
  int n = Config::GetConfig()->GetPartitionSize(par_id);
  return n;
}

int TapirCoord::GetSlowQuorum(parid_t par_id) {
  int n = Config::GetConfig()->GetPartitionSize(par_id);
  return n/2 + 1;
}


bool TapirCoord::AllSlowQuorumReached() {
  // verify(0);
  // currently the
  auto pars = cmd_->GetPartitionIds();
  bool all_slow_quorum_reached = true;
  for (auto& par_id : pars) {
    if (n_fast_accept_oks_[par_id] < GetSlowQuorum(par_id)) {
      all_slow_quorum_reached = false;
      break;
    }
  }
  return all_slow_quorum_reached;
}


bool TapirCoord::AllFastQuorumReached() {
  // verify(0);
  // currently the
  auto pars = cmd_->GetPartitionIds();
  bool all_fast_quorum_reached = true;
  for (auto& par_id : pars) {
    if (n_fast_accept_oks_[par_id] < GetFastQuorum(par_id)) {
      all_fast_quorum_reached = false;
      break;
    }
  }
  return all_fast_quorum_reached;
}

void TapirCoord::Decide() {
  phase_++;
  verify(decision_ != UNKNOWN);
  auto pars = cmd_->GetPartitionIds();
  Log_debug("send out decide request, cmd_id: %lx, ", cmd_->id_);
  for (auto par_id : pars) {
    commo()->BroadcastDecide(par_id, cmd_->id_, decision_);
  }
  if (decision_ == COMMIT) {
    Log_debug("commit and callback, cmd_id: %lx, ", cmd_->id_);
    TxnCommand* cmd = (TxnCommand*)cmd_;
    auto& txn_reply = cmd->get_reply();
    verify(cmd->callback_);
    cmd->callback_(txn_reply);
  } else if (decision_ == ABORT) {
    // TODO retry if abort.
    Reset();
    TxnCommand* cmd = (TxnCommand*)cmd_;
    cmd->retry();
    this->restart(cmd);
    verify(0);
  } else {
    verify(0);
  }
}

} // namespace rococo
