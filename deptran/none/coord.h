#pragma once

#include "../three_phase/coord.h"

namespace rococo {
class NoneCoord : public ThreePhaseCoordinator {
 public:
  using ThreePhaseCoordinator::ThreePhaseCoordinator;

  virtual void do_one(TxnRequest &);
  virtual void Handout();
  virtual void HandoutAck(phase_t phase, int res, Command& cmd);


};
} // namespace rococo
