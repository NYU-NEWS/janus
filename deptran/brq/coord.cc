
#include "../__dep__.h"
#include "../txn_chopper.h"
#include "frame.h"
#include "commo.h"
#include "coord.h"


namespace rococo {
//BrqCoord::BrqCoord(
//  uint32_t                  coo_id,
//  uint32_t                  thread_id,
//  bool                      batch_optimal) : coo_id_(coo_id),
//                                             thread_id_(thread_id),
//                                             batch_optimal_(batch_optimal) {
//  rpc_poll_ = new PollMgr(1);
//  recorder_ = nullptr;
//  if (Config::GetConfig()->do_logging()) {
//    std::string log_path(Config::GetConfig()->log_path());
//    log_path.append(std::to_string(coo_id_));
//    recorder_ = new Recorder(log_path.c_str());
//    rpc_poll_->add(recorder_);
//  }
//}
//
//void BrqCoord::launch(Command &cmd) {
//  cmd_ = cmd;
//  cmd_id_ = next_cmd_id();
//  this->FastAccept();
//}


BrqCommo* BrqCoord::commo() {
  if (commo_ == nullptr) {
    commo_ = frame_->CreateCommo();
  }
  verify(commo_ != nullptr);
  return dynamic_cast<BrqCommo*>(commo_);
}

void BrqCoord::launch_recovery(cmdid_t cmd_id) {
  // TODO
  prepare();
}

void BrqCoord::DispatchAck(phase_t phase,
                           int res,
                           SimpleCommand &cmd,
                           RccGraph &graph) {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  verify(phase == phase_); // cannot proceed without all acks.
  TxnInfo& info = *graph.vertex_index_.at(cmd.root_id_)->data_;
  verify(cmd.root_id_ == info.id());
  verify(info.partition_.find(cmd.partition_id_) != info.partition_.end());
  n_dispatch_ack_++;
  TxnCommand *txn = (TxnCommand *) cmd_;
  dispatch_acks_[cmd.inn_id_] = true;

  Log_debug("get start ack %ld/%ld for cmd_id: %lx, inn_id: %d",
            n_dispatch_ack_, n_dispatch_, txn->id_, cmd.inn_id_);

  // where should I store this graph?
  Log_debug("start response graph size: %d", (int)graph.size());
  verify(graph.size() > 0);
  graph_.Aggregate(graph);

  txn->Merge(cmd);

  if (txn->HasMoreSubCmdReadyNotOut()) {
    Log_debug("command has more sub-cmd, cmd_id: %lx,"
                  " n_started_: %d, n_pieces: %d",
              txn->id_, txn->n_pieces_out_, txn->GetNPieceAll());
    Dispatch();
  } else if (AllDispatchAcked()) {
    Log_debug("receive all start acks, txn_id: %llx; START PREPARE", cmd_->id_);
    GotoNextPhase();
  }
}


void BrqCoord::PreAccept() {
//  // generate fast accept request
  // set broadcast callback
  // broadcast
  for (auto par_id : cmd_->GetPartitionIds()) {
    commo()->BroadcastPreAccept(par_id,
                                cmd_->id_,
                                magic_ballot(),
                                graph_,
                                std::bind(&BrqCoord::PreAcceptAck,
                                          this,
                                          phase_,
                                          par_id,
                                          std::placeholders::_1,
                                          std::placeholders::_2));
  }
}

void BrqCoord::PreAcceptAck(phase_t phase,
                            parid_t par_id,
                            int res,
                            RccGraph& graph) {
  // if recevie more messages after already gone to next phase, ignore
  if (phase != phase_) return;
  n_fast_accpet_graphs_[par_id].push_back(graph);
  if (res == SUCCESS) {
    n_fast_accept_oks_[par_id]++;
  } else if (res == REJECT) {
    verify(0);
    n_fast_accept_rejects_[par_id]++;
  } else {
    verify(0);
  }
  if (FastpathPossible()) {
    // there is still chance for fastpath
    if (AllFastQuorumsReached()) {
      // receive enough identical replies to continue fast path.
      // go to the commit.
      committed_ = true;
      GotoNextPhase();
    } else {
      // skip, wait
    }
  } else {
    // fastpath is no longer a choice
    if (SlowpathPossible()) {
        if(SlowQuorumsAchieved()) {
          // go to the accept phase
          verify(0); // TODO
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
void BrqCoord::prepare() {
  // TODO
  // do not do failure recovery for now.
  verify(0);
}

void BrqCoord::accept() {
//  AcceptRequest request;
//  request.ballot = ballot_;
//  request.cmd_id = cmd_id_;
//  request.cmd = *cmd_;
//  // TODO
//  //request.deps = deps_;
//  auto phase = phase_;
//  auto callback = [this, phase] (groupid_t g, AcceptReply* reply) {
//    this->accept_ack(g, reply, phase);
//  };
//  auto timeout_callback = [this, phase] (groupid_t g) {
//    this->accept_ack(g, nullptr, phase);
//  };
}

//void BrqCoord::accept_ack(groupid_t g,
//                          AcceptReply* reply,
//                          phase_t phase) {
//  if (phase_ != phase) {
//    return;
//  }
//  auto &n = n_accept_reply_[g];
//  if (reply && reply->ack) {
//    n.yes++;
//  } else {
//    n.no++;
//  }
//  if (check_accept_possible()) {
//    if (check_accept()) {
//      // go to commit
//    }
//  } else {
//    // not handle this currently
//    verify(0);
//  }
//  // if have reached a quorum
//}

void BrqCoord::Commit() {
  TxnCommand *txn = (TxnCommand*) cmd_;
  TxnInfo& info = *graph_.FindV(cmd_->id_)->data_;
  verify(txn->partition_ids_.size() == info.partition_.size());
  info.union_status(TXN_CMT);
  for (auto par_id : cmd_->GetPartitionIds()) {
    commo()->BroadcastCommit(par_id,
                             cmd_->id_,
                             graph_,
                             std::bind(&BrqCoord::CommitAck,
                                       this,
                                       phase_,
                                       par_id,
                                       std::placeholders::_1,
                                       std::placeholders::_2));
  }
}

void BrqCoord::CommitAck(phase_t phase,
                         parid_t par_id,
                         int32_t res,
                         TxnOutput& output) {
  // TODO?
  if (phase != phase_) return;
  auto &n = n_commit_reply_[par_id];
  if (++(n.yes) > 1)
    return; // already seen outputs.

  TxnCommand* txn = (TxnCommand*) cmd_;
  txn->outputs_.insert(output.begin(), output.end());
  // if collect enough results.
  // if there are still more results to collect.
  GotoNextPhase();
  return;
}

bool BrqCoord::FastpathPossible() {
  auto pars = txn().GetPartitionIds();
  bool all_fast_quorum_possible = true;
  for (auto& par_id : pars) {
    auto par_size = Config::GetConfig()->GetPartitionSize(par_id);
    if (n_fast_accept_rejects_[par_id] > par_size - GetFastQuorum(par_id)) {
      all_fast_quorum_possible = false;
      break;
    }
    // TODO check graph.
    // if more than (par_size - fast quorum) graph is different, then nack.
    auto& vec_graph = n_fast_accpet_graphs_[par_id];
    int r = FastQuorumGraphCheck(par_id);
    if (r == 2)
      all_fast_quorum_possible = false;
  }
  return all_fast_quorum_possible;
};

int BrqCoord::GetFastQuorum(parid_t par_id) {
  int n = Config::GetConfig()->GetPartitionSize(par_id);
  return n;
}

bool BrqCoord::AllFastQuorumsReached() {
  auto pars = txn().GetPartitionIds();
  bool all_fast_quorum_reached = true;
  for (auto &par_id : pars) {
    int r = FastQuorumGraphCheck(par_id);
    if (r == 2) {
      return false;
    } else if (r == 1) {
      // do nothing
    } else if (r == 0) {
      verify(0);
    } else {
      verify(0);
    }
  }
  return all_fast_quorum_reached;
}

// return value
// 0: less than a fast quorum
// 1: a fast quorum of the same
// 2: >=(par_size - fast quorum) of different graphs.
int BrqCoord::FastQuorumGraphCheck(parid_t par_id) {
  verify(0);
  auto& vec_graph = n_fast_accpet_graphs_[par_id];
  auto par_size = Config::GetConfig()->GetPartitionSize(par_id);
  auto fast_quorum = GetFastQuorum(par_id);
  if (vec_graph.size() < fast_quorum)
    return 0;
  verify(vec_graph.size() >= 1);
  for (int i = 1; i < vec_graph.size(); i++) {
    RccGraph& graph = vec_graph[i];
    if (graph != vec_graph[0])
      return 2;
  }
  return 1;
}

void BrqCoord::GotoNextPhase() {
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
      phase_++; // FIXME
      verify(phase_ % n_phase == Phase::COMMIT);
      // TODO
      break;
    case Phase::ACCEPT:
      verify(0);
      verify(phase_ % n_phase == Phase::COMMIT);
    case Phase::COMMIT:
      verify(phase_ % n_phase == Phase::INIT_END);
      End();
      break;
    default:
      verify(0);
  }
}

} // namespace rococo
