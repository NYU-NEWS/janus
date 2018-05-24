
#pragma once

#include "../tx.h"

namespace janus {

class TxFebruus : public Tx {
 public:
  using Tx::Tx;
  uint64_t timestamp_ = 0;
  ballot_t max_seen_ballot_{0};
  ballot_t max_accepted_ballot_{0};
};

} // namespace janus
