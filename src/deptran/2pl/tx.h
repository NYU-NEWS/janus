#pragma once

#include "../classic/tx.h"

namespace janus {

using rrr::ALockGroup;

class Tx2pl: public TxClassic {
 public:
  vector<std::pair<ALock*, uint64_t>> locked_locks_ = {};
  bool woundable_{true};
  bool wounded_{false};

  Tx2pl(epoch_t epoch, txnid_t tid, TxLogServer *);
};

} // namespace janus
