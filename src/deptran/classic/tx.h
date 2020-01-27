#pragma once

#include "../tx.h"

namespace janus {

class TxClassic: public Tx {
 public:
  using Tx::Tx;
  shared_ptr<IntEvent> ev_prepare_{Reactor::CreateSpEvent<IntEvent>()};
  shared_ptr<IntEvent> ev_commit_{Reactor::CreateSpEvent<IntEvent>()};
  bool is_leader_hint_{false};
  bool result_prepare_{false};
};

} // namespace janus
