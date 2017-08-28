#pragma once

#include "../dtxn.h"

namespace janus {

using rrr::ALockGroup;

class Tx2pl: public Tx {
 public:
  vector<std::pair<ALock*, uint64_t>> locked_locks_ = {};
  bool prepared_{false};
  bool wounded_{false};

  Tx2pl(epoch_t epoch, txnid_t tid, Scheduler *);
};

} // namespace janus
