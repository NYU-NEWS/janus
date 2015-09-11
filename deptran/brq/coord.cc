#include "all.h"

namespace rococo {
BRQCoordinator::BRQCoordinator(
  uint32_t                  coo_id,
  std::vector<std::string>& addrs,
  int32_t                   benchmark,
  int32_t                   mode,
  ClientControlServiceImpl *ccsi,
  uint32_t                  thread_id,
  bool                      batch_optimal) : coo_id_(coo_id),
                                             benchmark_(benchmark),
                                             mode_(mode),
                                             ccsi_(ccsi),
                                             thread_id_(thread_id),
                                             batch_optimal_(batch_optimal),
                                             site_prepare_(addrs.size(), 0),
                                             site_commit_(addrs.size(), 0),
                                             site_abort_(addrs.size(), 0),
                                             site_piece_(addrs.size(), 0) {

  rpc_poll_ = new PollMgr(1);

  for (auto& addr : addrs) {
    rrr::Client *rpc_cli = new rrr::Client(rpc_poll_);
    Log::info("connect to site: %s", addr.c_str());
    auto ret = rpc_cli->connect(addr.c_str());
    verify(ret == 0);
    RococoProxy *rpc_proxy = new RococoProxy(rpc_cli);
    vec_rpc_cli_.push_back(rpc_cli);
    vec_rpc_proxy_.push_back(rpc_proxy);
    phase_ = 1;
  }
  recorder_ = NULL;
  if (Config::get_config()->do_logging()) {
    std::string log_path(Config::get_config()->log_path());
    log_path.append(std::to_string(coo_id_));
    recorder_ = new Recorder(log_path.c_str());
    rpc_poll_->add(recorder_);
  }
}

RequestHeader BRQCoordinator::gen_header(TxnChopper *ch) {
  RequestHeader header;

  header.cid    = coo_id_;
  header.tid    = ch->txn_id_;
  header.t_type = ch->txn_type_;
  return header;
}

void BRQCoordinator::launch(Command &cmd) {
  cmd_ = cmd;
}

void BRQCoordinator::launch_recovery(cmdid_t cmd_id) {
  prepare(cmd_id);
}

void BRQCoordinator::fast_accept() {
  ballotid_t ballot = get_special_ballot();
  // generate fast accept request
  FastAcceptReqeust request;
  request.cmd_id = next_cmd_id();
  request.ballot = ballot;
  request.cmd = cmd;
  // set broadcast callback
  auto callback = [this, phase_] (groupid_t g, FastAcceptReply* reply) {
    this->fast_accept_ack(g, reply, phase_);
  };
  auto timeout_callbak = [this, phase_] (groupdid_t g) {
    this->fast_accept_ack(g, nullptr, phase_);
  };
  // broadcast
  for (auto g : cmd.get_groups();) {
    Commo::broadcast(g, request, callback, timeout_callback);
  }
}

void BRQCoordinator::fast_accept_ack(groupid_t g, FastAcceptReply *reply,
                                     phase_t phase) {
  // if recevie more messages after already gone to next phase, ignore
  if (phase != phase_) {
    return;
  }
  auto& n = n_fast_accept_reply_[g];
  if (reply && reply.ack) {
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
void BRQCoordinator::prepare()) {
  // TODO
  // do not do failure recovery for now.
  verify(0);
}

void BRQCoordinator::accept() {
  AcceptRequest request;
  request.ballot = ballot_;
  request.cmd_id = cmd_id_;
  request.cmd = cmd;
  request.deps = deps_;
  auto callback = [this, phase_] (groupid_t g, AcceptReply* reply) {
    this->accept_ack(g, reply, phase_);
  }
  auto timeout_callback = [this, phase_] (groupid_t g) {
    this->accept_ack(g, nullptr, phase_);
  }
}

void BRQCoordinator::accept_ack(groupid_t g, AcceptReply* reply,
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

void BRQCoordinator::commit() {
  FastAcceptReqeust request;
  request.ballot = ballot_;
  request.cmd_id = cmd_id_;
  request.cmd = cmd;
  // set broadcast callback
  auto callback = [this, phase_] (groupid_t g, CommitReply* reply) {
    this->commit_ack(g, reply, phase_);
  };
  auto timeout_callbak = [this, phase_] (groupdid_t g) {
    this->commit_ack(g, nullptr, phase_);
  };
  // broadcast
  for (auto g : cmd.get_groups();) {
    Commo::broadcast(g, request, callback, timeout_callback);
  }
}

void BRQCoordinator::commit_ack(groupid_t g, CommitReply* reply,
                                phase_t phase) {
  if (phase != phase_)
    return;
  if (commit_ack[g])
    return; // already seen outputs.

  cmd_.feed(reply->output);
  n_commit++;
  if (n_commit == commit_ack.size) {
    phase_++;
    // TODO command callback.
  }
  // if collect enough results.
  // if there are still more results to collect.
  return;
}
} // namespace rococo
