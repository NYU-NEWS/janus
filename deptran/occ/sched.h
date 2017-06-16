#pragma once

#include "../classic/sched.h"

namespace rococo {

class OCCSched: public ClassicSched {
 public:
  OCCSched();
  virtual mdb::Txn *get_mdb_txn(const i64 tid);

  virtual bool HandleConflicts(DTxn& dtxn,
                               innid_t inn_id,
                               vector<string>& conflicts) {
    verify(0);
  };
};

} // namespace rococo