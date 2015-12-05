#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../three_phase/exec.h"

namespace rococo {

class TPLExecutor: public ThreePhaseExecutor {
 using ThreePhaseExecutor::ThreePhaseExecutor;

};

} // namespace rococo