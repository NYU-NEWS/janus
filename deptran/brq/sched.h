
#pragma once
#include "../scheduler.h"
#include "../rcc/sched.h"

namespace rococo {

class BrqSched : public RccSched {
  using RccSched::RccSched;
};

} // namespace rococo