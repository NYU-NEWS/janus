#pragma once

#include "../classic/coord.h"

namespace rococo {

class RccGraph;

class RccCoord: public ThreePhaseCoordinator {

protected:
  inline RococoCommunicator* comm() {
    return static_cast<RococoCommunicator*>(commo_);
  }

public:
  RccCoord(uint32_t coo_id,
           int benchmark,
           ClientControlServiceImpl *ccsi,
           uint32_t thread_id)
      : ThreePhaseCoordinator(coo_id,
                              benchmark,
                              ccsi,
                              thread_id) {
  }

  void do_one(TxnRequest&) override;

  void Handout();
  void HandoutAck(phase_t phase,
                          int res,
                          Command& cmd,
                          RccGraph& graph);
  void Finish();
  void FinishAck(phase_t phase, int res);

  void FinishRo();
};
} // namespace rococo
