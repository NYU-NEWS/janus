#pragma once

#include "../classic/coord.h"

namespace rococo {
class NoneCoord : public ThreePhaseCoordinator {
 public:
  using ThreePhaseCoordinator::ThreePhaseCoordinator;

  virtual void do_one(TxnRequest &);
  virtual void Dispatch() override;
  virtual void DispatchAck(phase_t phase, int res, Command &cmd);


};
} // namespace rococo
