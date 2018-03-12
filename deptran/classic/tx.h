#pragma once

#include "../tx.h"

namespace janus {

class TxClassic: public Tx {
 public:
  using Tx::Tx;
  IntEvent ev_prepare_{};
  IntEvent ev_commit_{};
  bool result_prepare;
};

} // namespace janus
