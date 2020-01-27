#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../command.h"
#include "../rcc/coord.h"

namespace janus {
class TroadCommo;
class CoordinatorTroad : public RccCoord {
 public:
  enum Phase {
    INIT_END = 0, DISPATCH = 1, PREPARE = 2,
    PRE_ACCEPT = 3, ACCEPT = 4, COMMIT = 5
  };

  bool __debug_notifying_{false};
  cooid_t coo_id_;
//  phase_t  phase_; // a phase identifier
  uint32_t thread_id_;
  uint32_t cmdid_prefix_c_;
  Recorder *recorder_;
  ballot_t ballot_ = 0; // the ballot I am holding
  // data structures for saving replies.
  struct reply_cnt_t { int yes; int no; };
  map<parid_t, int> n_fast_accept_oks_{};
//  map<parid_t, int> n_accept_oks_{};
//  map<parid_t, int> n_fast_accept_rejects_ = {};
  map<parid_t, vector<shared_ptr<RccGraph>>> n_fast_accept_graphs_{};
  map<parid_t, int> fast_accept_graph_check_caches_{};
  bool fast_path_ = false;

//  map<groupid_t, reply_cnt_t> n_fast_accept_reply_;
//  map<groupid_t, reply_cnt_t> n_accept_reply_;
  map<groupid_t, reply_cnt_t> n_prepare_reply_;
//  map<groupid_t, reply_cnt_t> n_commit_reply_;

  using RccCoord::RccCoord;

  virtual ~CoordinatorTroad() {}

  TroadCommo *commo();
  // Dispatch inherits from RccCoord;
  void DispatchRo() override { DispatchAsync(); }

  void PreAccept();

  // do_one inherits from RccCoord;

  void restart() { verify(0); };
  // functions needed in the fast accept phase.
  int32_t GetFastQuorum(parid_t par_id);
  int32_t GetSlowQuorum(parid_t par_id);
  bool PreAcceptAllSlowQuorumsReached();

  void prepare();
  // functions needed in the accept phase.
  void Accept();

  void Commit() override;
  void NotifyValidation();

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
  bool FastQuorumGraphCheck(PreAcceptQuorumEvent& );
  void GotoNextPhase() override;
  void Reset() override;
};
} // namespace janus
