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
  map<txid_t, ParentEdge<RccTx>> parents_{};
  rank_t rank_{RANK_UNDEFINED};
  enum RoState { BEGIN, FIRST, SECOND, DONE };
  enum Phase { INIT_END = 0, DISPATCH, COMMIT };

  RoState ro_state_ = BEGIN;
  map<int32_t, mdb::version_t> last_vers_;
  map<int32_t, mdb::version_t> curr_vers_;

  bool fast_commit_{false};
  map<parid_t, int> n_commit_oks_{};
  bool mocking_janus_{false};

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
};
} // namespace janus
