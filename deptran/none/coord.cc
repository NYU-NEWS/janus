
#include "none/coord.h"
#include "frame.h"
#include "benchmark_control_rpc.h"

namespace rococo {

/** thread safe */

void NoneCoord::GotoNextPhase() {

  int n_phase = 2;
  int current_phase = phase_ % n_phase;
  switch (phase_++ % n_phase) {
    case Phase::INIT_END:
      Dispatch();
      verify(phase_ % n_phase == Phase::DISPATCH);
      break;
    case Phase::DISPATCH:
      committed_ = true;
      verify(phase_ % n_phase == Phase::INIT_END);
      End();
      break;
    default:
      verify(0);
  }
}

} // namespace rococo
