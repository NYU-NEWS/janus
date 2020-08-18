
#include "../__dep__.h"
#include "deptran/procedure.h"
#include "frame.h"
#include "commo.h"
#include "coordinator.h"

namespace janus {

TroadCommo* CoordinatorTroad::commo() {
  if (commo_ == nullptr) {
    commo_ = frame_->CreateCommo(nullptr);
    commo_->loc_id_ = loc_id_;
  }
  verify(commo_ != nullptr);
  return dynamic_cast<TroadCommo*>(commo_);
}

void CoordinatorTroad::launch_recovery(cmdid_t cmd_id) {
  // TODO
  prepare();
}

void CoordinatorTroad::PreAccept() {
  WAN_WAIT;
  std::lock_guard<std::recursive_mutex> guard(mtx_);
//  auto dtxn = sp_graph_->FindV(cmd_->id_);
//  verify(tx_data().partition_ids_.size() == dtxn->partition_.size());
  vector<shared_ptr<PreAcceptQuorumEvent>> events;
  auto& partitions = GetTxPartitions(*cmd_, rank_);
  for (auto par_id : partitions) {
    auto cmds = tx_data().GetCmdsByPartitionAndRank(par_id, rank_);
    verify(!cmds.empty());
    verify(rank_ != RANK_UNDEFINED);
    Log_debug("broadcast pre-accept partition %d", (int) par_id);
    auto ev = commo()->BroadcastPreAccept(par_id,
                                          cmd_->id_,
                                          rank_,
                                    magic_ballot(),
                                       cmds);
    events.push_back(ev);
  }
  bool fast_path = true;
  for (const auto& ev: events) {
    ev->Wait();
    verify(ev->Yes()); // TODO tolerate failure recovery.
    if (!FastQuorumGraphCheck(*ev)) {
      fast_path = false;
    }
  }
  parents_.clear();
  if (fast_path) {
    fast_path_ = true;
    for (auto ev : events) {
      auto& g = ev->vec_parents_[0];
      MergeParents(*g);
    }
  } else {
    fast_path_ = false;
    for (auto ev: events) {
      for (auto g : ev->vec_parents_) {
//        sp_graph_->Aggregate(0, *sp_graph);
        MergeParents(*g);
      }
    }
  }
  GotoNextPhase();
}

/** caller should be thread_safe */
void CoordinatorTroad::prepare() {
  // TODO
  // do not do failure recovery for now.
  verify(0);
}

void CoordinatorTroad::Accept() {
  WAN_WAIT;
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  verify(!fast_path_);
//  Log_info("broadcast accept request for txn_id: %llx", cmd_->id_);
//  TxData* txn = (TxData*) cmd_;
//  auto dtxn = sp_graph_->FindV(cmd_->id_);
//  verify(txn->partition_ids_.size() == dtxn->partition_.size());
//  sp_graph_->UpgradeStatus(*dtxn, TXN_CMT);
  vector<shared_ptr<QuorumEvent>> events;
  auto& partitions = GetTxPartitions(*cmd_, rank_);
  for (auto par_id : partitions) {
    auto ev = commo()->BroadcastAccept(par_id,
                             cmd_->id_,
                             rank_,
                             ballot_,
                             parents_);
    events.push_back(ev);
  }

  for (auto ev : events) {
    ev->Wait(100*1000*1000);
    verify(ev->Yes()); // TODO handle failure recovery.
  }
  GotoNextPhase();
}

void CoordinatorTroad::Commit() {
  if (rank_ == RANK_I) {
    verify(!mocking_janus_);
    sp_ev_commit_->Set(1);
    ReportCommit();
  }
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  auto* txn = (TxData*) cmd_;
//  auto dtxn = sp_graph_->FindV(cmd_->id_);
//  verify(txn->partition_ids_.size() == dtxn->partition_.size());
//  sp_graph_->UpgradeStatus(*dtxn, TXN_CMT);
  vector<shared_ptr<QuorumEvent>> events;
  auto& partitions = GetTxPartitions(*cmd_, rank_);
  for (auto par_id : partitions) {
    auto ev = commo()->BroadcastCommit(par_id,
                                       cmd_->id_,
                                       rank_,
                                       txn->need_validation_,
                                       parents_);
    events.push_back(ev);
  }
  aborted_ = false;
  for (auto i = 0 ; i < events.size(); i++) {
    auto &ev = events[i];
    ev->Wait(100 * 1000 * 1000);
    verify(ev->status_ != Event::TIMEOUT);
    if (ev->No()) {
      verify(0);
    } else if (ev->Yes()) {

    } else {
      // TODO fixing this, bug here: server does not return within a timeout
      verify(0);
    }
  }
  if (mocking_janus_) {
    NotifyValidation();
  } else {
    committed_ = !aborted_;
    GotoNextPhase();
  }
//  txn().Merge(output);
  // if collect enough results.
  // if there are still more results to collect.
//  GotoNextPhase();
//  bool all_acked = txn().OutputReady();
//  if (all_acked)
//  GotoNextPhase();
}

void CoordinatorTroad::NotifyValidation() {
//  __debug_notifying_ = true;
  verify(phase_ % 6 == COMMIT);
  verify(rank_ == RANK_D);
  auto& partitions = GetTxPartitions(*cmd_, rank_);
  auto ev1 = commo()->CollectValidation(cmd_->id_, partitions);
  ev1->Wait(100*1000*1000);
  verify(ev1->status_ != Event::TIMEOUT);
  int res;
  if (ev1->Yes()) {
    res = SUCCESS;
  } else if (ev1->No()){
    verify(mocking_janus_);
    aborted_ = true;
    res = REJECT;
  } else {
    verify(0);
  }
  committed_ = !aborted_;
  verify(par_d_ == tx_data().GetPartitionIds());
  auto ev = commo()->BroadcastValidation(*cmd_, res);
//  ev->Wait();
//  verify(ev->Yes());
//  verify(phase_ % 6 == COMMIT);
//  __debug_notifying_ = false;
  GotoNextPhase();
}

int32_t CoordinatorTroad::GetFastQuorum(parid_t par_id) {
  int32_t n = Config::GetConfig()->GetPartitionSize(par_id);
  return n;
}

int32_t CoordinatorTroad::GetSlowQuorum(parid_t par_id) {
  int32_t n = Config::GetConfig()->GetPartitionSize(par_id);
  return n / 2 + 1;
}

/**
 *
 * @param par_id
 * @param vec_graph
 * @return true if fast quorum acheived, false if not
 */
bool CoordinatorTroad::FastQuorumGraphCheck(PreAcceptQuorumEvent& ev) {
  // TODO optimize for redundant element in parent_set?
  verify(ev.vec_parents_.size() > 0);
  auto& sp_parents = ev.vec_parents_[0];
  for (int i = 1; i < ev.vec_parents_.size(); i++) {
    auto& sp_parents2 = ev.vec_parents_[i];
    if (*sp_parents != *sp_parents2) return false;
  }
  return true;
//  auto par_id = ev.partition_id_;
//  auto& vec_graph = ev.graphs_;
//  auto fast_quorum = GetFastQuorum(par_id);
//  verify(vec_graph.size() >= fast_quorum);
////  verify(vec_graph.size() == 1);
////  verify(vec_graph.size() >= 1);
//  verify(vec_graph.size() == fast_quorum);
//  auto v = vec_graph[0]->FindV(cmd_->id_);
//  verify(v != nullptr);
//  auto& parent_set = v->GetParents();
//  auto sz = parent_set.size();
//  for (int i = 1; i < vec_graph.size(); i++) {
//    RccGraph& graph = *vec_graph[i];
//    auto vv = graph.FindV(cmd_->id_);
//    auto& pp_set = vv->GetParents();
//    if (parent_set != pp_set) {
//      return false;
//    }
//  }
  return true;
}

void CoordinatorTroad::GotoNextPhase() {
  int n_phase = 6;
  int last_phase = phase_ % n_phase; // for debug
  phase_++;
  if (rank_ == RANK_UNDEFINED) {
    if (mocking_janus_) {
      rank_ = RANK_D;
    } else {
      rank_ = RANK_I;
    }
  }
  switch (last_phase % n_phase) {
    case Phase::INIT_END:
      PreDispatch();
      verify(phase_ % n_phase == Phase::DISPATCH);
      break;
    case Phase::DISPATCH:
      verify(phase_ % n_phase == Phase::PREPARE);
      phase_++; // skip prepare for leader, TODO recovery coord.
      verify(phase_ % n_phase == Phase::PRE_ACCEPT);
      if (par_i_.empty() && rank_ == RANK_I) {
        rank_ = RANK_D;
      }
//      if (SKIP_I) {
//        rank_ = RANK_D;
//      }
      PreAccept();
      break;
    case Phase::PRE_ACCEPT:
      if (SKIP_I && rank_ == RANK_I) {
        rank_ = RANK_D;
        phase_ = Phase::DISPATCH;
        GotoNextPhase();
        return;
      }
      verify(phase_ % n_phase == Phase::ACCEPT);
      if (fast_path_) {
        phase_++;
        verify(phase_ % n_phase == Phase::COMMIT);
        Commit();
      } else {
        verify(phase_ % n_phase == Phase::ACCEPT);
        Accept();
      }
      break;
    case Phase::ACCEPT:
      verify(phase_ % n_phase == Phase::COMMIT);
      Commit();
      break;
    case Phase::COMMIT:
      verify(phase_ % n_phase == Phase::INIT_END);
      verify(committed_ != aborted_);
      if (committed_) {
        if (rank_ == RANK_D) {
          End();
        } else if (rank_ == RANK_I) {
          rank_ = RANK_D;
          phase_+=1; // skip dispatch prepare
          verify(phase_ % n_phase == Phase::DISPATCH);
          GotoNextPhase(); // directly to PRE_ACCEPT
        }
      } else if (aborted_) {
        verify(rank_ == RANK_D && mocking_janus_);
        Restart();
      } else {
        verify(0);
      }
      break;
    default:
      verify(0);
  }
}

void CoordinatorTroad::Reset() {
  RccCoord::Reset();
  fast_path_ = false;
//  n_accept_oks_.clear();
//  n_fast_accept_rejects_.clear();
//  fast_accept_graph_check_caches_.clear();
  rank_ = RANK_UNDEFINED;
}
} // namespace janus
