
#include "../__dep__.h"
#include "deptran/procedure.h"
#include "frame.h"
#include "commo.h"
#include "coordinator.h"

namespace janus {

JanusCommo* CoordinatorJanus::commo() {
  if (commo_ == nullptr) {
    commo_ = frame_->CreateCommo(nullptr);
    commo_->loc_id_ = loc_id_;
  }
  verify(commo_ != nullptr);
  return dynamic_cast<JanusCommo*>(commo_);
}

void CoordinatorJanus::launch_recovery(cmdid_t cmd_id) {
  // TODO
  prepare();
}

void CoordinatorJanus::PreAccept() {
  std::lock_guard<std::recursive_mutex> guard(mtx_);
//  // generate fast accept request
  // set broadcast callback
  // broadcast
//  auto dtxn = sp_graph_->FindV(cmd_->id_);
//  verify(tx_data().partition_ids_.size() == dtxn->partition_.size());
//  for (auto par_id : cmd_->GetPartitionIds()) {
//    auto cmds = tx_data().GetCmdsByPartition(par_id);
//    commo()->BroadcastPreAccept(par_id,
//                                cmd_->id_,
//                                magic_ballot(),
//                                cmds,
//                                sp_graph_,
//                                std::bind(&CoordinatorJanus::PreAcceptAck,
//                                          this,
//                                          phase_,
//                                          par_id,
//                                          std::placeholders::_1,
//                                          std::placeholders::_2));
//  }
}

void CoordinatorJanus::PreAcceptAck(phase_t phase,
                                    parid_t par_id,
                                    int res,
                                    shared_ptr<RccGraph> graph) {
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  // if recevie more messages after already gone to next phase, ignore
  if (phase != phase_) return;
  verify(graph != nullptr);
//  verify(graph->FindV(txn().root_id_) != nullptr);
//  verify(n_fast_accept_graphs_.size() == 0);
  n_fast_accept_graphs_[par_id].push_back(graph);
  if (res == SUCCESS) {
    n_fast_accept_oks_[par_id]++;
  } else if (res == REJECT) {
    verify(0);
//    n_fast_accept_rejects_[par_id]++;
  } else {
    verify(0);
  }
  if (FastpathPossible()) {
    // there is still chance for fastpath
    if (AllFastQuorumsReached()) {
      // receive enough identical replies to continue fast path.
      // go to the commit.
      fast_path_ = true;
//      Log_info("pre acked success on txn_id: %llx", cmd_->id_);
      ChooseGraph();
      GotoNextPhase();
    } else {
      // skip, wait
    }
  } else {
    // fastpath is no longer a choice
    if (SlowpathPossible()) {
      if (PreAcceptAllSlowQuorumsReached()) {
        verify(!fast_path_);
        GotoNextPhase();
      } else {
        // skip, wait
      }
    } else {
      // slowpath is not a choice either.
      // not handle for now.
      verify(0);
    }
  }
}

/** caller should be thread_safe */
void CoordinatorJanus::prepare() {
  // TODO
  // do not do failure recovery for now.
  verify(0);
}

void CoordinatorJanus::ChooseGraph() {
//  for (auto& pair : n_fast_accept_graphs_) {
//    auto& vec_graph = pair.second;
//    if (fast_path_) {
//      auto& g = vec_graph[0];
//      sp_graph_->Aggregate(0, *g);
//    } else {
//      for (auto g : vec_graph) {
//        sp_graph_->Aggregate(0, *g);
//      }
//    }
//  }
}

void CoordinatorJanus::Accept() {
//  std::lock_guard<std::recursive_mutex> guard(mtx_);
//  verify(!fast_path_);
////  Log_info("broadcast accept request for txn_id: %llx", cmd_->id_);
//  ChooseGraph();
//  TxData* txn = (TxData*) cmd_;
//  auto dtxn = sp_graph_->FindV(cmd_->id_);
//  verify(txn->partition_ids_.size() == dtxn->partition_.size());
//  sp_graph_->UpgradeStatus(*dtxn, TXN_CMT);
//  for (auto par_id : cmd_->GetPartitionIds()) {
//    commo()->BroadcastAccept(par_id,
//                             cmd_->id_,
//                             ballot_,
//                             sp_graph_,
//                             std::bind(&CoordinatorJanus::AcceptAck,
//                                       this,
//                                       phase_,
//                                       par_id,
//                                       std::placeholders::_1));
//  }
}

void CoordinatorJanus::AcceptAck(phase_t phase,
                                 parid_t par_id,
                                 int res) {
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  if (phase_ != phase) {
    return;
  }
  verify(res == SUCCESS);
  n_accept_oks_[par_id]++;

  if (AcceptQuorumPossible()) {
    if (AcceptQuorumReached()) {
      GotoNextPhase();
    } else {
      // skip;
    }
  } else {
    // not handle this currently
    verify(0);
  }
  // if have reached a quorum
}

void CoordinatorJanus::Commit() {
//  std::lock_guard<std::recursive_mutex> guard(mtx_);
//  TxData* txn = (TxData*) cmd_;
//  auto dtxn = sp_graph_->FindV(cmd_->id_);
//  verify(txn->partition_ids_.size() == dtxn->partition_.size());
//  sp_graph_->UpgradeStatus(*dtxn, TXN_CMT);
//  for (auto par_id : cmd_->GetPartitionIds()) {
//    commo()->BroadcastCommit(par_id,
//                             cmd_->id_,
//                             RANK_UNDEFINED,
//                             txn->need_validation_,
//                             sp_graph_,
//                             std::bind(&CoordinatorJanus::CommitAck,
//                                       this,
//                                       phase_,
//                                       par_id,
//                                       std::placeholders::_1,
//                                       std::placeholders::_2));
//  }
//  if (fast_commit_) {
//    verify(0);
//    committed_ = true;
//    GotoNextPhase();
//  }
//  if (txn->need_validation_) {
//    auto pars = cmd_->GetPartitionIds();
//    auto quorum = commo()->BroadcastInquireValidation(pars, cmd_->id_);
//    quorum->Wait();
//    int result = 0;
//    if (quorum->Yes()) {
//      result = 1;
//      validation_result_ = true;
//    } else if (quorum->No()) {
//      result = -1;
//      validation_result_ = false;
//    } else {
//      verify(0);
//    }
//    commo()->BroadcastNotifyValidation(cmd_->id_, pars, result);
//  }
}

void CoordinatorJanus::CommitAck(phase_t phase,
                                 parid_t par_id,
                                 int32_t res,
                                 TxnOutput& output) {
  // TODO fix this.
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  if (phase != phase_) return;
  if (fast_commit_) return;
  if (res == SUCCESS) {
    committed_ = true;
  } else if (res == REJECT) {
    aborted_ = true;
  } else {
    verify(0);
  }
  n_commit_oks_[par_id]++;

  // TODO. Right now wait until all commit request returning before commit, fix it.
  commo()->rpc_par_proxies_[par_id].size();
  for (auto pid : cmd_->GetPartitionIds()) {
    if (n_commit_oks_[pid] < commo()->rpc_par_proxies_[pid].size()) {
      return;
    }
  }

//  txn().Merge(output);
  // if collect enough results.
  // if there are still more results to collect.
  GotoNextPhase();
//  bool all_acked = txn().OutputReady();
//  if (all_acked)
//  GotoNextPhase();
}

bool CoordinatorJanus::FastpathPossible() {
  auto pars = tx_data().GetPartitionIds();
  bool all_fast_quorum_possible = true;
  for (auto& par_id : pars) {
    auto par_size = Config::GetConfig()->GetPartitionSize(par_id);
//    if (n_fast_accept_rejects_[par_id] > par_size - GetFastQuorum(par_id)) {
//      all_fast_quorum_possible = false;
//      verify(0);
//      break;
//    }
    // TODO check graph.
    // if more than (par_size - fast quorum) graph is different, then nack.
    int r = FastQuorumGraphCheck(par_id);
    if (r == 1 || r == 3) {

    } else if (r == 2) {
      all_fast_quorum_possible = false;
    }
  }
  return all_fast_quorum_possible;
};

int32_t CoordinatorJanus::GetFastQuorum(parid_t par_id) {
  int32_t n = Config::GetConfig()->GetPartitionSize(par_id);
  return n;
}

int32_t CoordinatorJanus::GetSlowQuorum(parid_t par_id) {
  int32_t n = Config::GetConfig()->GetPartitionSize(par_id);
  return n / 2 + 1;
}

bool CoordinatorJanus::AllFastQuorumsReached() {
  auto pars = tx_data().GetPartitionIds();
  for (auto& par_id : pars) {
    int r = FastQuorumGraphCheck(par_id);
    if (r == 2) {
      return false;
    } else if (r == 1) {
      // do nothing
    } else if (r == 3) {
      // do nothing
      return false;
    } else {
      verify(0);
    }
  }
  return true;
}

bool CoordinatorJanus::AcceptQuorumReached() {
  auto pars = tx_data().GetPartitionIds();
  for (auto& par_id : pars) {
    if (n_fast_accept_oks_[par_id] < GetSlowQuorum(par_id)) {
      return false;
    }
  }
  return true;
}

bool CoordinatorJanus::PreAcceptAllSlowQuorumsReached() {
  auto pars = cmd_->GetPartitionIds();
  bool all_slow_quorum_reached =
      std::all_of(pars.begin(),
                  pars.end(),
                  [this](parid_t par_id) {
                    return n_fast_accept_oks_[par_id] >= GetSlowQuorum(par_id);
                  });
  return all_slow_quorum_reached;
};

// return value
// 1: a fast quorum of the same
// 2: >=(par_size - fast quorum) of different graphs. fast quorum not possible.
// 3: less than a fast quorum graphs received.
int CoordinatorJanus::FastQuorumGraphCheck(parid_t par_id) {
  verify(0);
  /*
  auto par_size = Config::GetConfig()->GetPartitionSize(par_id);
  auto& vec_graph = n_fast_accept_graphs_[par_id];
  auto fast_quorum = GetFastQuorum(par_id);
  if (vec_graph.size() < fast_quorum)
    return 3;
  int res = fast_accept_graph_check_caches_[par_id];
  if (res > 0) return res;

  res = 1;
//  verify(vec_graph.size() == 1);
//  verify(vec_graph.size() >= 1);
  verify(vec_graph.size() == fast_quorum);
  auto v = vec_graph[0]->FindV(cmd_->id_);
  verify(v != nullptr);
  auto& parent_set = v->GetParents();
  for (int i = 1; i < vec_graph.size(); i++) {
    RccGraph& graph = *vec_graph[i];
    auto vv = graph.FindV(cmd_->id_);
    auto& pp_set = vv->GetParents();
    if (parent_set != pp_set) {
      res = 2;
      break;
    }
  }
  fast_accept_graph_check_caches_[par_id] = res;
  return res;
   */
}

void CoordinatorJanus::GotoNextPhase() {
  int n_phase = 6;
  int current_phase = phase_ % n_phase; // for debug
  switch (phase_++ % n_phase) {
    case Phase::INIT_END:
      PreDispatch();
      verify(phase_ % n_phase == Phase::DISPATCH);
      break;
    case Phase::DISPATCH:
      phase_++;
      verify(phase_ % n_phase == Phase::PRE_ACCEPT);
      PreAccept();
      break;
    case Phase::PRE_ACCEPT:
      if (fast_path_) {
        phase_++; // FIXME
        verify(phase_ % n_phase == Phase::COMMIT);
        Commit();
      } else {
        verify(phase_ % n_phase == Phase::ACCEPT);
        Accept();
      }
      // TODO
      break;
    case Phase::ACCEPT:
      verify(phase_ % n_phase == Phase::COMMIT);
      Commit();
      break;
    case Phase::COMMIT:
      verify(phase_ % n_phase == Phase::INIT_END);
      verify(committed_ != aborted_);
      if (committed_) {
        if (validation_result_) {
          End();
        } else {
          aborted_ = true;
          Restart();
        }
      } else if (aborted_) {
        Restart();
      } else {
        verify(0);
      }
      break;
    default:
      verify(0);
  }
}

void CoordinatorJanus::Reset() {
  RccCoord::Reset();
  fast_path_ = false;
  n_fast_accept_graphs_.clear();
  n_fast_accept_oks_.clear();
  n_accept_oks_.clear();
//  n_fast_accept_rejects_.clear();
  fast_accept_graph_check_caches_.clear();
  validation_result_ = true;
}
} // namespace janus
