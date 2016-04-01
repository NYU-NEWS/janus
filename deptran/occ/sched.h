//
// Created by shuai on 11/25/15.
//

#pragma once

#include "../classic/sched.h"

namespace rococo {

class OCCSched: public ClassicSched {
 public:
  OCCSched();
  virtual mdb::Txn *get_mdb_txn(const i64 tid);
//  virtual mdb::Txn *get_mdb_txn(const RequestHeader &req);
};

} // namespace rococo