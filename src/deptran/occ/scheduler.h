#pragma once

#include "deptran/classic/scheduler.h"

namespace janus {

class SchedulerOcc: public SchedulerClassic {
 public:
  SchedulerOcc();
  virtual mdb::Txn *get_mdb_txn(const i64 tid);

  virtual bool HandleConflicts(Tx& dtxn,
                               innid_t inn_id,
                               vector<string>& conflicts) override {
    verify(0);
  };
  virtual bool DispatchPiece(Tx& tx,
                             TxPieceData& cmd,
                             TxnOutput& ret_output) override {
    SchedulerClassic::DispatchPiece(tx, cmd, ret_output);
    ExecutePiece(tx, cmd, ret_output);
    return true;
  }
  virtual bool Guard(Tx &tx_box, Row *row, int col_id, bool write) override {
    // TODO? read write guard?
//    Log_fatal("before access not implemented for occ");
    return false;
  };
  virtual bool DoPrepare(txnid_t tx_id) override;
  virtual void DoCommit(Tx& tx) override;
};

} // namespace janus
