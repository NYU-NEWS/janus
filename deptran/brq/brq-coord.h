#pragma once

#include "all.h"
#include "constants.h"
#include "command.h"
#include "brq-graph.h"
#include "brq-common.h"
#include "brq-commo.h"

namespace rococo {
class BRQCoordinator {
public:
  rrr::PollMgr *rpc_poll_;

  cooid_t  coo_id_;
  phase_t  phase_; // a phase identifier
  uint32_t thread_id_;
  bool batch_optimal_;
  uint32_t cmdid_prefix_c_;
  Mutex mtx_;
  Recorder *recorder_;
  ballot_t ballot_; // the ballot I am holding
  cmdid_t cmd_id_;
  Command cmd_;
  BRQCommo *commo_;
  // data structures for saving replies.
  struct reply_cnt_t {int yes; int no;};
  std::map<groupid_t, reply_cnt_t> n_fast_accept_reply_;
  std::map<groupid_t, reply_cnt_t> n_accept_reply_;
  std::map<groupid_t, reply_cnt_t> n_prepare_reply_;
  std::map<groupid_t, reply_cnt_t> n_commit_reply_;
  BRQGraph deps_;

  BRQCoordinator(uint32_t coo_id,
                 uint32_t thread_id = 0,
                 bool batch_optimal = false);

  virtual ~BRQCoordinator() {}

  void restart();
  // functions needed in the fast accept phase.
  void FastAccept();
  void fast_accept_ack(groupid_t, FastAcceptReply*, phase_t);
  bool check_fastpath_possible();
  bool check_fastpath();
  bool check_slowpath_possible();
  bool check_slowpath();

  void prepare();
  // functions needed in the accept phase.
  void accept();
  void accept_ack(groupid_t, AcceptReply*, phase_t);
  bool check_accept_possible();
  bool check_accept();

  void commit();
  void commit_ack(groupid_t, CommitReply*, phase_t);
  bool check_commit();

  void reset(); // reuse for next cmd.

  void launch(Command &cmd);
  void launch_recovery(cmdid_t cmd_id);

  ballot_t magic_ballot() {
    ballot_t ret = 0;
    ret = (ret << 32) | coo_id_;
    return ret;
  }

  cmdid_t next_cmd_id() {
    cmdid_t ret = cmdid_prefix_c_++;
    ret = (ret << 32 | coo_id_);
    return ret;
  }
};
} // namespace rococo
