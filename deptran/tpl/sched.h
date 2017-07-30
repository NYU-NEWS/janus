//
// Created by shuai on 11/25/15.
//

#pragma once

#include "../classic/sched.h"

namespace rococo {

class Executor;
class TPLSched: public ClassicSched {
 public:
  TPLSched();

  virtual mdb::Txn *get_mdb_txn(const i64 tid);
  virtual mdb::Txn *del_mdb_txn(const i64 tid);

  virtual bool HandleConflicts(DTxn& dtxn,
                               innid_t inn_id,
                               vector<string>& conflicts) {
    verify(0);
  };

  virtual bool BeforeAccess(TxBox& tx_box, Row* row, int col_idx) override;

  virtual bool DoPrepare(txnid_t tx_id) override;

  virtual void DoCommit(TxBox& tx_box) override;

  virtual void DoAbort(TxBox& tx_box) override;
};

} // namespace rococo