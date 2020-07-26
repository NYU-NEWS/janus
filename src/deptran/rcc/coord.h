#pragma once

#include "dep_graph.h"
#include "deptran/classic/coordinator.h"

namespace janus {

class RccCommo;

class RccCoord : public CoordinatorClassic {

 public:
  RccCommo* commo();

 public:
//  shared_ptr<RccGraph> sp_graph_{new RccGraph};
  parent_set_t parents_{};
  rank_t rank_{RANK_UNDEFINED};
  enum RoState { BEGIN, FIRST, SECOND, DONE };
  enum Phase { INIT_END = 0, DISPATCH, COMMIT };

  RoState ro_state_ = BEGIN;
  map<int32_t, mdb::version_t> last_vers_;
  map<int32_t, mdb::version_t> curr_vers_;

  bool fast_commit_{false};
  map<parid_t, int> n_commit_oks_{};
  bool mocking_janus_{false};

  set<parid_t> par_i_{};
  set<parid_t> par_d_{};

  RccCoord(uint32_t coo_id,
           int benchmark,
           ClientControlServiceImpl* ccsi,
           uint32_t thread_id)
      : CoordinatorClassic(coo_id,
                           benchmark,
                           ccsi,
                           thread_id) {
  }

  void PreDispatch();
  void DispatchAsync() override ;
  virtual void DispatchAck(phase_t phase,
                           int res,
                           TxnOutput& cmd,
                           RccGraph& graph);
  void Finish();
  void FinishAck(phase_t phase,
                 int res,
                 map<innid_t, map<int32_t, Value>>& output);

  void Commit() override;
  virtual void CommitAck(phase_t phase,
                 parid_t par_id,
                 int32_t res,
                 TxnOutput &output);

  virtual void DispatchRo();
  void DispatchRoAck(phase_t phase,
                     int res,
                     SimpleCommand& cmd,
                     map<int, mdb::version_t>& vers);
  void FinishRo() { verify(0); };
  void Reset() override;
  virtual void GotoNextPhase() override ;

  set<parid_t>& GetTxPartitions(CmdData& cmd, rank_t rank) {
    verify(rank == RANK_I || rank == RANK_D);
    if (rank == RANK_I) {
      verify(!par_i_.empty());
//      verify(par_i_.size() == 1);
      return par_i_;
    } else {
      return par_d_;
    }
  }

  void MergeParents(parent_set_t& s2) {
    auto& s1 = parents_;
    for (auto& pair : s2) {
      // TODO reduce size
      s1.push_back(pair);
      verify(pair.first != tx_data().id_);
    }
  }

};
} // namespace janus
