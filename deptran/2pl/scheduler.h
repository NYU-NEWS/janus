//
// Created by shuai on 11/25/15.
//

#pragma once

#include "deptran/classic/scheduler.h"

namespace rococo {

class Executor;
class Scheduler2pl: public SchedulerClassic {
 public:
  Scheduler2pl();

  virtual mdb::Txn *get_mdb_txn(const i64 tid);
  virtual mdb::Txn *del_mdb_txn(const i64 tid);

  virtual bool HandleConflicts(Tx& dtxn,
                               innid_t inn_id,
                               vector<string>& conflicts) {
    verify(0);
  };

  virtual bool Guard(Tx &tx_box, Row *row, int col_idx, bool write) override;

  virtual bool DoPrepare(txnid_t tx_id) override;

  virtual void DoCommit(Tx& tx_box) override;

  virtual void DoAbort(Tx& tx_box) override;
};

} // namespace rococo