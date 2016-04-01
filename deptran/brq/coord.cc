
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
  n_handout_ack_++;
  TxnCommand *txn = (TxnCommand *) cmd_;
  handout_acks_[cmd.inn_id_] = true;

  Log_debug("get start ack %ld/%ld for cmd_id: %lx, inn_id: %d",
            n_handout_ack_, n_handout_, txn->id_, cmd.inn_id_);

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
    PreAccept();
  }
}


void BrqCoord::PreAccept() {
//  // generate fast accept request
  // set broadcast callback
  auto phase = phase_;
  // broadcast
  for (auto par_id : cmd_->GetPartitionIds()) {
    // TODO
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
                            parid_t gid,
                            int res,
                            RccGraph& graph) {
  // if recevie more messages after already gone to next phase, ignore
  if (phase != phase_) {
    return;
  }
  auto& n = n_fast_accept_reply_[gid];
  if (res == SUCCESS) {
    n.yes++;
  } else {
    verify(0);
    n.no++;
  }
  if (FastpathPossible()){
    // there is still chance for fastpath
    if (FastQuorumsAchieved()) {
      // receive enough identical replies to continue fast path.
      // go to the commit.
      phase_++;
      Commit();
    } else {
      // skip, wait
    }
  } else {
    // fastpath is no longer a choice
    if (SlowpathPossible()) {
        if(SlowQuorumsAchieved()) {
          // go to the accept phase
          phase_++;
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

void BrqCoord::accept_ack(groupid_t g, AcceptReply* reply,
                          phase_t phase) {
  if (phase_ != phase) {
    return;
  }
  auto &n = n_accept_reply_[g];
  if (reply && reply->ack) {
    n.yes++;
  } else {
    n.no++;
  }
  if (check_accept_possible()) {
    if (check_accept()) {
      // go to commit
    }
  } else {
    // not handle this currently
    verify(0);
  }
  // if have reached a quorum
}

void BrqCoord::Commit() {
  TxnCommand *txn = (TxnCommand*) cmd_;
  TxnInfo& info = *graph_.FindV(cmd_->id_)->data_;
  verify(txn->partition_ids_.size() == info.partition_.size());
  info.union_status(TXN_CMT);
  for (auto g : cmd_->GetPartitionIds()) {
    commo()->BroadcastCommit(g,
                             cmd_->id_,
                             graph_,
                             std::bind(&BrqCoord::CommitAck,
                                       this,
                                       phase_,
                                       g,
                                       std::placeholders::_1));
  }
}

void BrqCoord::CommitAck(phase_t phase,
                         parid_t g,
                         map<innid_t, map<int32_t, Value>>& output) {
  // TODO?
  if (phase != phase_)
    return;
  auto &n = n_commit_reply_[g];
  if (++(n.yes) > 1)
    return; // already seen outputs.

  TxnCommand* txn = (TxnCommand*) cmd_;
  txn->outputs_.insert(output.begin(), output.end());
  if (check_commit()) {
    phase_++;
    // TODO command callback.
    txn->reply_.res_ = SUCCESS;
    TxnReply& txn_reply_buf = txn->get_reply();
    double    last_latency  = txn->last_attempt_latency();
    this->report(txn_reply_buf, last_latency
#ifdef TXN_STAT
        , ch
#endif // ifdef TXN_STAT
    );
    txn->callback_(txn_reply_buf);
  }
  // if collect enough results.
  // if there are still more results to collect.
  return;
}

bool BrqCoord::FastpathPossible() {
  // TODO
  return true;
};

bool BrqCoord::FastQuorumsAchieved() {
  // TODO
//  verify(0);
  return true;
}

} // namespace rococo
