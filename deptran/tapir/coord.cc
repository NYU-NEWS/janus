#include "../__dep__.h"
#include "commo.h"
#include "coord.h"
#include "frame.h"

namespace rococo {

TapirCommo* TapirCoord::commo() {
//  verify(commo_ != nullptr);
  if (commo_ == nullptr) {
    commo_ = new TapirCommo();
  }
  return (TapirCommo*)commo_;
}

void TapirCoord::do_one(TxnRequest& req) {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  verify(txn_reg_);
  TxnCommand *cmd = frame_->CreateTxnCommand(req, txn_reg_);
  verify(txn_reg_ != nullptr);
  cmd_ = cmd;
  cmd_->root_id_ = this->next_txn_id();
  cmd_->id_ = cmd_->root_id_;
  Reset(); // In case of reuse.
  Log_debug("do one request txn_id: %lx\n", cmd_->id_);
  if (ccsi_) ccsi_->txn_start_one(thread_id_, cmd->type_);
  FastAccept();
}

void TapirCoord::Reset() {
  Coordinator::Reset();
  n_accept_oks_.clear();
  n_fast_accept_oks_.clear();
}

void TapirCoord::FastAccept() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  phase_++;
  while (cmd_->HasMoreSubCmdReadyNotOut()) {
    auto subcmd = (SimpleCommand*)cmd_->GetNextSubCmd();
    subcmd->id_ = next_pie_id();
    verify(subcmd->root_id_ == cmd_->id_);
    n_start_++;
    Log_debug("send out fast accept request %ld, cmd_id: %lx, "
                  "inn_id: %d, pie_id: %lx",
              n_start_, cmd_->id_, subcmd->inn_id_, subcmd->id_);
    start_ack_map_[subcmd->inn_id()] = false;
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
