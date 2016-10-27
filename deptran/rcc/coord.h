#pragma once

#include "dep_graph.h"
#include "../classic/coord.h"

namespace rococo {

class RccCommo;

class RccCoord: public ClassicCoord {

public:
  RccCommo* commo();

public:
  RccGraph graph_{};
  enum RoState {BEGIN, FIRST, SECOND, DONE};
  enum Phase {INIT_END=0, DISPATCH, COMMIT};

  RoState ro_state_ = BEGIN;
  map<int32_t, mdb::version_t> last_vers_;
  map<int32_t, mdb::version_t> curr_vers_;

  RccCoord(uint32_t coo_id,
           int benchmark,
           ClientControlServiceImpl *ccsi,
           uint32_t thread_id)
      : ClassicCoord(coo_id,
                              benchmark,
                              ccsi,
                              thread_id), graph_() {
    verify(graph_.size() == 0);
  }

  void PreDispatch();
  void Dispatch();
  virtual void DispatchAck(phase_t phase,
                           int res,
                           TxnOutput &cmd,
                           RccGraph &graph);
  void Finish();
  void FinishAck(phase_t phase,
                 int res,
                 map<innid_t, map<int32_t, Value>>& output);

  virtual void DispatchRo();
  void DispatchRoAck(phase_t phase,
                     int res,
                     SimpleCommand &cmd,
                     map<int, mdb::version_t> &vers);
  void FinishRo() {verify(0);};
  void Reset() override;
  virtual void GotoNextPhase();
};
} // namespace rococo
