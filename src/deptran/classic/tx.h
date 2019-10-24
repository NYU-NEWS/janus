#pragma once

#include "../tx.h"

namespace janus {

class TxClassic: public Tx {
 public:
  using Tx::Tx;
  IntEvent& ev_prepare_{Reactor::CreateEvent<IntEvent>()};
  IntEvent& ev_commit_{Reactor::CreateEvent<IntEvent>()};
  bool is_leader_hint_{false};
  bool result_prepare_{false};
};

} // namespace janus
