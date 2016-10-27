#include "../__dep__.h"
#include "coord.h"
#include "frame.h"
#include "commo.h"
#include "benchmark_control_rpc.h"
#include "exec.h"

namespace rococo {

TapirCommo* TapirCoord::commo() {
//  verify(commo_ != nullptr);
  if (commo_ == nullptr) {
    commo_ = new TapirCommo();
    commo_->loc_id_ = loc_id_;
    verify(loc_id_ < 100);
  }
  auto commo = dynamic_cast<TapirCommo*>(commo_);
  verify(commo != nullptr);
  return commo;
}

void TapirCoord::Dispatch() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  //  ___TestPhaseOne(cmd_id_);
  auto txn = (TxnCommand*) cmd_;

  int cnt = 0;
  auto cmds_by_par = txn->GetReadyCmds();

  for (auto& pair: cmds_by_par) {
    const parid_t& par_id = pair.first;
    vector<SimpleCommand*>& cmds = pair.second;
    n_dispatch_ += cmds.size();
    cnt += cmds.size();
    vector<SimpleCommand> cc;
    for (SimpleCommand* c: cmds) {
      c->id_ = next_pie_id();
      dispatch_acks_[c->inn_id_] = false;
      cc.push_back(*c);
    }
    commo()->SendDispatch(cc,
                          this,
                          std::bind(&ClassicCoord::DispatchAck,
                                    this,
                                    phase_,
                                    std::placeholders::_1,
                                    std::placeholders::_2));
  }
  Log_debug("sent %d SubCmds\n", cnt);
}


void TapirCoord::DispatchAck(phase_t phase,
                             int32_t res,
                             TxnOutput& outputs) {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  if (phase != phase_) return;
  TxnCommand *txn = (TxnCommand *) cmd_;
  for (auto& pair : outputs) {
    n_dispatch_ack_++;
    const innid_t& inn_id = pair.first;
    verify(dispatch_acks_[inn_id] == false);
    dispatch_acks_[inn_id] = true;
    Log_debug("get start ack %ld/%ld for cmd_id: %lx, inn_id: %d",
              n_dispatch_ack_, n_dispatch_, cmd_->id_, inn_id);

    txn->Merge(pair.first, pair.second);
  }
  if (txn->HasMoreSubCmdReadyNotOut()) {
    Log_debug("command has more sub-cmd, cmd_id: %llx,"
                  " n_started_: %d, n_pieces: %d",
              txn->id_, txn->n_pieces_dispatched_, txn->GetNPieceAll());
    Dispatch();
  } else if (AllDispatchAcked()) {
    Log_debug("receive all start acks, txn_id: %llx; START PREPARE",
              txn->id_);
    GotoNextPhase();
  }
}

void TapirCoord::Reset() {
  ClassicCoord::Reset();
  dispatch_acks_.clear();
  n_accept_oks_.clear();
  n_fast_accept_oks_.clear();
  n_fast_accept_rejects_.clear();
}

void TapirCoord::FastAccept() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);

  Log_debug("send out fast accept for cmd_id: %llx", cmd_->id_);
  auto pars = txn().GetPartitionIds();
  verify(pars.size() > 0);
  int32_t sum = 0;
  for (auto par_id : pars) {
    vector<SimpleCommand> txn_cmds = txn().GetCmdsByPartition(par_id);
    sum += txn_cmds.size();
    verify(txn_cmds.size() > 0);
    verify(txn_cmds.size() < 10000);
    n_fast_accept_oks_[par_id] = 0;
    n_fast_accept_rejects_[par_id] = 0;
    commo()->BroadcastFastAccept(par_id,
                                 txn().id_,
                                 txn_cmds,
                                 std::bind(&TapirCoord::FastAcceptAck,
                                           this,
                                           phase_,
                                           par_id,
                                           std::placeholders::_1));
  }
  verify(sum == txn().cmds_.size());
}

void TapirCoord::FastAcceptAck(phase_t phase,
                               parid_t par_id,
                               int32_t res) {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  if (phase_ > phase) return;
  // if for every partition, get fast quorum of OK, go into decide.
  // do not consider anything else for now.
//  verify(res == SUCCESS); // TODO
  if (res == SUCCESS) {
    n_fast_accept_oks_[par_id]++;
  } else if (res == REJECT) {
    n_fast_accept_rejects_[par_id]++;
  } else {
    verify(0);
  }
  if (FastQuorumPossible()) {
    if (AllFastQuorumReached()) {
      committed_ = true;
      GotoNextPhase();
    } else {
      // do nothing and wait for future ack.
    }
  } else {
    aborted_ = true;
    GotoNextPhase();
  }
}

bool TapirCoord::FastQuorumPossible() {
  auto pars = cmd_->GetPartitionIds();
  bool all_fast_quorum_possible = true;
  for (auto& par_id : pars) {
    auto par_size = Config::GetConfig()->GetPartitionSize(par_id);
    if (n_fast_accept_rejects_[par_id] > par_size-GetFastQuorum(par_id)) {
      all_fast_quorum_possible = false;
      break;
    }
  }
  return all_fast_quorum_possible;
}

void TapirCoord::Accept() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
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
  if (phase_ != phase) return;
  int res;
  fu->get_reply() >> res;
  verify(res == SUCCESS);
  n_accept_oks_[pid]++;
  if (n_accept_oks_[pid] >= GetSlowQuorum(pid)) {
    GotoNextPhase();
  } else {
    // TODO keep waiting.
  }
}

void TapirCoord::Restart() {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  cmd_->root_id_ = this->next_txn_id();
  cmd_->id_ = cmd_->root_id_;
  ClassicCoord::Restart();
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
  verify(committed_ != aborted_);
  int32_t d = 0;
  if (committed_) {
    d = Decision::COMMIT;
  } else if (aborted_) {
    d = Decision::ABORT;
  } else {
    verify(0);
  }
  auto pars = cmd_->GetPartitionIds();
  Log_debug("send out decide request, cmd_id: %llx, ", cmd_->id_);
  for (auto par_id : pars) {
    commo()->BroadcastDecide(par_id, cmd_->id_, d);
  }
  GotoNextPhase();
}

void TapirCoord::GotoNextPhase() {
  int n_phase = 4;
  switch (phase_++ % n_phase) {
    case Phase::INIT_END:
      Dispatch();
      verify(phase_ % n_phase == Phase::DISPATCH);
      break;
    case Phase::DISPATCH:
      verify(phase_ % n_phase == Phase::FAST_ACCEPT);
      FastAccept();
      break;
    case Phase::FAST_ACCEPT:
      verify(phase_ % n_phase == Phase::DECIDE);
      Decide();
      break;
    case Phase::DECIDE:
      verify(phase_ % n_phase == Phase::INIT_END);
      if (committed_)
        End();
      else if (aborted_)
        Restart();
      else
        verify(0);
      break;
    default:
      verify(0);
  }
}

} // namespace rococo
