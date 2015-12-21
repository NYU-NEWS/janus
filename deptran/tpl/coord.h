#pragma once

#include "../three_phase/coord.h"

namespace rococo {

class TPLCoord: public ThreePhaseCoordinator {
  using ThreePhaseCoordinator::ThreePhaseCoordinator;
};

} // namespace rococo