
#include "../__dep__.h"
#include "../txn_chopper.h"
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

void BrqCoord::launch_recovery(cmdid_t cmd_id) {
  // TODO
  prepare();
}


void BrqCoord::DispatchAck(phase_t phase,
                           int res,
                           SimpleCommand &cmd,
                           RccGraph &graph) {
  verify(0);
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  verify(phase == phase_);
  n_handout_ack_++;
  TxnCommand *txn = (TxnCommand *) cmd_;
  handout_acks_[cmd.inn_id_] = true;

  Log_debug("get start ack %ld/%ld for cmd_id: %lx, inn_id: %d",
            n_handout_ack_, n_handout_, txn->id_, cmd.inn_id_);

  bool early_return = false;

  // where should I store this graph?
  Log_debug("start response graph size: %d", (int)graph.size());
  verify(graph.size() > 0);
  graph_.Aggregate(graph);

//  Log_debug(
//      "receive deptran start response, tid: %llx, pid: %llx, graph size: %d",
//      cmd.root_id_,
//      cmd->id_,
//      gra.size());

  // TODO?
  if (graph.size() > 1) txn->disable_early_return();

  txn->n_pieces_out_++;
  txn->Merge(cmd);

  if (txn->HasMoreSubCmdReadyNotOut()) {
    Log_debug("command has more sub-cmd, cmd_id: %lx,"
                  " n_started_: %d, n_pieces: %d",
              txn->id_, txn->n_pieces_out_, txn->GetNPieceAll());
    Dispatch();
  }
}

void BrqCoord::FastAccept() {
  // generate fast accept request
  FastAcceptRequest request;
  request.cmd_id = cmd_id_;
  request.cmd = *cmd_;
  request.ballot = magic_ballot();
  // set broadcast callback
  auto phase = phase_;
  auto callback = [this, phase] (groupid_t g, FastAcceptReply* reply) {
    this->FastAcceptAck(g, reply, phase);
  };
  // broadcast
  for (auto g : cmd_->GetPartitionIds()) {
    // TODO
    commo_->broadcast_fast_accept(g, request, callback);
  }
}

void BrqCoord::FastAcceptAck(groupid_t g, FastAcceptReply *reply,
                             phase_t phase) {
  // if recevie more messages after already gone to next phase, ignore
  if (phase != phase_) {
    return;
  }
  auto& n = n_fast_accept_reply_[g];
  if (reply && reply->ack) {
    n.yes++;
  } else {
    n.no++;
  }
  if (!check_fastpath_possible()){
    // there is still chance for fastpath
    if (check_fastpath()) {
      // receive enough identical replies to continue fast path.
      // go to the commit.
      phase_++;
    } else {
      // skip, wait
    }
  } else {
    // fastpath is no longer a choice
    if (check_slowpath_possible()) {
        if(check_slowpath()) {
          // go to the accept phase
          phase_++;
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
  AcceptRequest request;
  request.ballot = ballot_;
  request.cmd_id = cmd_id_;
  request.cmd = *cmd_;
  // TODO
  //request.deps = deps_;
  auto phase = phase_;
  auto callback = [this, phase] (groupid_t g, AcceptReply* reply) {
    this->accept_ack(g, reply, phase);
  };
  auto timeout_callback = [this, phase] (groupid_t g) {
    this->accept_ack(g, nullptr, phase);
  };
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

void BrqCoord::commit() {
  CommitRequest request;
  request.ballot = ballot_;
  request.cmd_id = cmd_id_;
  request.cmd = *cmd_;
  // set broadcast callback
  auto phase = phase_;
  auto callback = [this, phase] (groupid_t g, CommitReply* reply) {
    this->commit_ack(g, reply, phase);
  };
  // broadcast
  for (auto g : cmd_->GetPartitionIds()) {
    // TODO
    commo_->broadcast_commit(g, request, callback);
  }
}

void BrqCoord::commit_ack(groupid_t g, CommitReply* reply,
                          phase_t phase) {
  if (phase != phase_)
    return;
  auto &n = n_commit_reply_[g];
  if (++(n.yes) > 1)
    return; // already seen outputs.

  cmd_->Merge(reply->output);
  if (check_commit()) {
    phase_++;
    // TODO command callback.
  }
  // if collect enough results.
  // if there are still more results to collect.
  return;
}
} // namespace rococo
