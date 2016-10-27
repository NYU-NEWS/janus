#pragma once

#include "../classic/sched.h"

namespace rococo {

class OCCSched: public ClassicSched {
 public:
  OCCSched();
  virtual mdb::Txn *get_mdb_txn(const i64 tid);
};

} // namespace rococo