#pragma once

#include "dep_graph.h"
#include "../classic/coord.h"

namespace rococo {

class RccCommo;

class RccCoord: public ThreePhaseCoordinator {

public:
  RccCommo* commo() {
    return (RccCommo*)(commo_);
  }

public:
  RccGraph graph_;
  enum RoState {BEGIN, FIRST, SECOND, DONE};

  RoState ro_state_ = BEGIN;
  map<int32_t, mdb::version_t> last_vers_;
  map<int32_t, mdb::version_t> curr_vers_;

  RccCoord(uint32_t coo_id,
           int benchmark,
           ClientControlServiceImpl *ccsi,
           uint32_t thread_id)
      : ThreePhaseCoordinator(coo_id,
                              benchmark,
                              ccsi,
                              thread_id), graph_() {
  }

  void do_one(TxnRequest&) override;

  void Handout();
  virtual void HandoutAck(phase_t phase,
                  int res,
                  SimpleCommand& cmd,
                  RccGraph& graph);
  void Finish();
  void FinishAck(phase_t phase,
                 int res,
                 map<int, map<int32_t, Value>>& output);

  void HandoutRo();
  void HandoutRoAck(phase_t phase,
                    int res,
                    SimpleCommand& cmd,
                    map<int, mdb::version_t>& vers);
  void FinishRo() {verify(0);};
};
} // namespace rococo
