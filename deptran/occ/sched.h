//
// Created by shuai on 11/25/15.
//

#pragma once

#include "../scheduler.h"

namespace rococo {

class OCCSched: public Scheduler {
 public:
  OCCSched();
  virtual mdb::Txn *get_mdb_txn(const i64 tid);
  virtual mdb::Txn *get_mdb_txn(const RequestHeader &req);
};

} // namespace rococo