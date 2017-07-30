#pragma once

#include "../classic/coord.h"

namespace janus {
class NoneCoord : public ClassicCoord {
 public:
  using ClassicCoord::ClassicCoord;
  enum Phase {INIT_END=0, DISPATCH=1};
  void GotoNextPhase() override;
};
} // namespace janus
