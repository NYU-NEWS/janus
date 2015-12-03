//
// Created by shuai on 11/25/15.
//

#pragma once

#include "../scheduler.h"

namespace rococo {

class TPLSched: public Scheduler {
 public:
  TPLSched();
  virtual mdb::Txn *get_mdb_txn(const i64 tid);
  virtual mdb::Txn *get_mdb_txn(const RequestHeader &req);
  virtual mdb::Txn *del_mdb_txn(const i64 tid);
};

} // namespace rococo