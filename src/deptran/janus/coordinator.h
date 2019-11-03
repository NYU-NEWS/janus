#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../command.h"
#include "../rcc/coord.h"

namespace janus {
class JanusCommo;
class CoordinatorJanus : public RccCoord {
 public:
  enum Phase {
    INIT_END = 0, DISPATCH = 1, PREPARE = 2,
    PRE_ACCEPT = 3, ACCEPT = 4, COMMIT = 5
  };

  cooid_t coo_id_;
//  phase_t  phase_; // a phase identifier
  uint32_t thread_id_;
  uint32_t cmdid_prefix_c_;
  Recorder *recorder_;
  ballot_t ballot_ = 0; // the ballot I am holding
  // data structures for saving replies.
  struct reply_cnt_t { int yes; int no; };
  map<parid_t, int> n_fast_accept_oks_{};
  map<parid_t, int> n_accept_oks_{};
//  map<parid_t, int> n_fast_accept_rejects_ = {};
  map<parid_t, vector<shared_ptr<RccGraph>>> n_fast_accept_graphs_{};
  map<parid_t, int> fast_accept_graph_check_caches_{};
  bool fast_path_ = false;

//  map<groupid_t, reply_cnt_t> n_fast_accept_reply_;
//  map<groupid_t, reply_cnt_t> n_accept_reply_;
  map<groupid_t, reply_cnt_t> n_prepare_reply_;
//  map<groupid_t, reply_cnt_t> n_commit_reply_;

  using RccCoord::RccCoord;

  virtual ~CoordinatorJanus() {}

  JanusCommo *commo();
  // Dispatch inherits from RccCoord;
  void DispatchRo() override { DispatchAsync(); }

  void PreAccept();
  void PreAcceptAck(phase_t phase,
                    parid_t par_id,
                    int res,
                    shared_ptr<RccGraph> graph);
  // functions needed in the fast accept phase.
  bool FastpathPossible();
  bool AllFastQuorumsReached();
  bool SlowpathPossible() {
    // TODO without failures, slow path should always be possible.
    return true;
  };
  int32_t GetFastQuorum(parid_t par_id);
  int32_t GetSlowQuorum(parid_t par_id);
  bool PreAcceptAllSlowQuorumsReached();

  void prepare();
  // functions needed in the accept phase.
  void ChooseGraph();
  void Accept();
  void AcceptAck(phase_t phase, parid_t par_id, int res);
  bool AcceptQuorumPossible() {
    return true;
  };
  bool AcceptQuorumReached();

  void Commit() override;
  void CommitAck(phase_t phase,
                 parid_t par_id,
                 int32_t res,
                 TxnOutput &output) override;
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
  int FastQuorumGraphCheck(parid_t par_id);
  void GotoNextPhase() override;
  void Reset() override;
};
} // namespace janus
