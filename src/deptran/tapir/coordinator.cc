#include "../__dep__.h"
#include "coordinator.h"
#include "frame.h"
#include "commo.h"
#include "benchmark_control_rpc.h"

namespace janus {

TapirCommo *CoordinatorTapir::commo() {
//  verify(commo_ != nullptr);
  if (commo_ == nullptr) {
    commo_ = new TapirCommo();
    commo_->loc_id_ = loc_id_;
    verify(loc_id_ < 100);
  }
  auto commo = dynamic_cast<TapirCommo *>(commo_);
  verify(commo != nullptr);
  return commo;
}

void CoordinatorTapir::DispatchAsync() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  //  ___TestPhaseOne(cmd_id_);
  auto tx_data = (TxData *) cmd_;

  int cnt = 0;
  auto cmds_by_par = tx_data->GetReadyPiecesData();

  for (auto &pair: cmds_by_par) {
    const parid_t &par_id = pair.first;
    auto& cmds = pair.second;
    n_dispatch_ += cmds.size();
    cnt += cmds.size();
    auto sp_vec_pieces = std::make_shared<vector<shared_ptr<TxPieceData>>>();
    for (auto& c: cmds) {
      c->id_ = next_pie_id();
      dispatch_acks_[c->inn_id_] = false;
      sp_vec_pieces->push_back(c);
    }
    commo()->BroadcastDispatch(this->coo_id_, sp_vec_pieces,
                               this,
                               std::bind(&CoordinatorClassic::DispatchAck,
                                         this,
                                         phase_,
                                         std::placeholders::_1,
                                         std::placeholders::_2));
  }
  Log_debug("sent %d SubCmds", cnt);
}

void CoordinatorTapir::DispatchAck(phase_t phase,
                                   int32_t res,
                                   TxnOutput &outputs) {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  if (phase != phase_) return;
  TxData *txn = (TxData *) cmd_;
  for (auto &pair : outputs) {
    n_dispatch_ack_++;
    const innid_t &inn_id = pair.first;
    verify(dispatch_acks_[inn_id] == false);
    dispatch_acks_[inn_id] = true;
    Log_debug("get start ack %ld/%ld for cmd_id: %lx, inn_id: %d",
              n_dispatch_ack_, n_dispatch_, cmd_->id_, inn_id);

    txn->Merge(pair.first, pair.second);
  }
  if (txn->HasMoreUnsentPiece()) {
    Log_debug("command has more sub-cmd, cmd_id: %llx,"
                  " n_started_: %d, n_pieces: %d",
              txn->id_, txn->n_pieces_dispatched_, txn->GetNPieceAll());
    DispatchAsync();
  } else if (AllDispatchAcked()) {
    Log_debug("receive all start acks, txn_id: %llx; START PREPARE",
              txn->id_);
    GotoNextPhase();
  }
}

void CoordinatorTapir::Reset() {
  CoordinatorClassic::Reset();
  dispatch_acks_.clear();
  n_accept_oks_.clear();
  n_fast_accept_oks_.clear();
  n_fast_accept_rejects_.clear();
}

void CoordinatorTapir::FastAccept() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);

  Log_debug("send out fast accept for cmd_id: %llx", cmd_->id_);
  auto pars = tx_data().GetPartitionIds();
  verify(pars.size() > 0);
  int32_t sum = 0;
  for (auto par_id : pars) {
    vector<SimpleCommand> txn_cmds = tx_data().GetCmdsByPartition(par_id);
    sum += txn_cmds.size();
    verify(txn_cmds.size() > 0);
    verify(txn_cmds.size() < 10000);
    n_fast_accept_oks_[par_id] = 0;
    n_fast_accept_rejects_[par_id] = 0;
    commo()->BroadcastFastAccept(par_id,
                                 tx_data().id_,
                                 txn_cmds,
                                 std::bind(&CoordinatorTapir::FastAcceptAck,
                                           this,
                                           phase_,
                                           par_id,
                                           std::placeholders::_1));
  }
  verify(sum == tx_data().map_piece_data_.size());
}

void CoordinatorTapir::FastAcceptAck(phase_t phase,
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

bool CoordinatorTapir::FastQuorumPossible() {
  auto pars = cmd_->GetPartitionIds();
  bool all_fast_quorum_possible = true;
  for (auto &par_id : pars) {
    auto par_size = Config::GetConfig()->GetPartitionSize(par_id);
    if (n_fast_accept_rejects_[par_id] > par_size - GetFastQuorum(par_id)) {
      all_fast_quorum_possible = false;
      break;
    }
  }
  return all_fast_quorum_possible;
}

void CoordinatorTapir::Accept() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  ballot_t ballot = MAGIC_SACCEPT_BALLOT;
  auto decision = ABORT;
  for (auto par_id : cmd_->GetPartitionIds()) {
    commo()->BroadcastAccept(par_id,
                             cmd_->id_,
                             ballot,
                             decision,
                             std::bind(&CoordinatorTapir::AcceptAck,
                                       this,
                                       phase_,
                                       par_id,
                                       std::placeholders::_1));
  }
}

void CoordinatorTapir::AcceptAck(phase_t phase, parid_t pid, Future *fu) {
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

void CoordinatorTapir::Restart() {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  cmd_->root_id_ = this->next_txn_id();
  cmd_->id_ = cmd_->root_id_;
  CoordinatorClassic::Restart();
}

int CoordinatorTapir::GetFastQuorum(parid_t par_id) {
  int n = Config::GetConfig()->GetPartitionSize(par_id);
  return n;
}

int CoordinatorTapir::GetSlowQuorum(parid_t par_id) {
  int n = Config::GetConfig()->GetPartitionSize(par_id);
  return n / 2 + 1;
}

bool CoordinatorTapir::AllSlowQuorumReached() {
  // verify(0);
  // currently the
  auto pars = cmd_->GetPartitionIds();
  bool all_slow_quorum_reached = true;
  for (auto &par_id : pars) {
    if (n_fast_accept_oks_[par_id] < GetSlowQuorum(par_id)) {
      all_slow_quorum_reached = false;
      break;
    }
  }
  return all_slow_quorum_reached;
}

bool CoordinatorTapir::AllFastQuorumReached() {
  // verify(0);
  auto pars = cmd_->GetPartitionIds();
  bool all_fast_quorum_reached = true;
  for (auto &par_id : pars) {
    if (n_fast_accept_oks_[par_id] < GetFastQuorum(par_id)) {
      all_fast_quorum_reached = false;
      break;
    }
  }
  return all_fast_quorum_reached;
}

void CoordinatorTapir::Decide() {
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

void CoordinatorTapir::GotoNextPhase() {
  int n_phase = 4;
  switch (phase_++ % n_phase) {
    case Phase::INIT_END:DispatchAsync();
      verify(phase_ % n_phase == Phase::DISPATCH);
      break;
    case Phase::DISPATCH:verify(phase_ % n_phase == Phase::FAST_ACCEPT);
      FastAccept();
      break;
    case Phase::FAST_ACCEPT:verify(phase_ % n_phase == Phase::DECIDE);
      Decide();
      break;
    case Phase::DECIDE:verify(phase_ % n_phase == Phase::INIT_END);
      if (committed_)
        End();
      else if (aborted_)
        Restart();
      else
        verify(0);
      break;
    default:verify(0);
  }
}

} // namespace janus
