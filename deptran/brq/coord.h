#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../command.h"
#include "../rcc/coord.h"
#include "dep_graph.h"
#include "brq-common.h"

namespace rococo {
class BrqCommo;
class BrqCoord : public RccCoord {
public:
  cooid_t  coo_id_;
//  phase_t  phase_; // a phase identifier
  uint32_t thread_id_;
  uint32_t cmdid_prefix_c_;
  Recorder *recorder_;
  ballot_t ballot_; // the ballot I am holding
  cmdid_t cmd_id_;
  BrqCommo *commo_;
  // data structures for saving replies.
  struct reply_cnt_t {int yes; int no;};
  std::map<groupid_t, reply_cnt_t> n_fast_accept_reply_;
  std::map<groupid_t, reply_cnt_t> n_accept_reply_;
  std::map<groupid_t, reply_cnt_t> n_prepare_reply_;
  std::map<groupid_t, reply_cnt_t> n_commit_reply_;

  using RccCoord::RccCoord;

  virtual ~BrqCoord() {}

  // Dispatch inherits from RccCoord;
//  void DispatchAck(phase_t phase,
//                   int res,
//                   SimpleCommand &cmd,
//                   RccGraph &graph);

  // do_one inherits from RccCoord;

  void restart() {verify(0);};
  // functions needed in the fast accept phase.
  void FastAccept();
  void FastAcceptAck(groupid_t, FastAcceptReply *, phase_t);
  bool check_fastpath_possible() {verify(0);};
  bool check_fastpath() {verify(0);};
  bool check_slowpath_possible() {
    verify(0);
    return false;
  };
  bool check_slowpath() {
    verify(0);
    return false;
  };

  void prepare();
  // functions needed in the accept phase.
  void accept();
  void accept_ack(groupid_t, AcceptReply*, phase_t);
  bool check_accept_possible() {
    verify(0);
    return false;
  };
  bool check_accept() {
    verify(0);
    return false;
  };

  void commit();
  void commit_ack(groupid_t, CommitReply*, phase_t);
  bool check_commit() {
    verify(0);
    return false;
  };

//  void launch(Command &cmd);
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
