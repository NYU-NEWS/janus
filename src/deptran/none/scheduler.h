#pragma once

#include "../classic/scheduler.h"

namespace janus {

class SchedulerNone: public SchedulerClassic {
 using SchedulerClassic::SchedulerClassic;
 public:
    bool Dispatch(cmdid_t cmd_id,
                  shared_ptr<Marshallable> cmd,
                  TxnOutput& ret_output) override {
        bool res = SchedulerClassic::Dispatch(cmd_id, cmd, ret_output);
        DestroyTx(cmd_id);
        return res;
    }

  virtual bool DispatchPiece(Tx& tx,
                             TxPieceData& cmd,
                             TxnOutput& ret_output) override {
    SchedulerClassic::DispatchPiece(tx, cmd, ret_output);
    ExecutePiece(tx, cmd, ret_output);
    return true;
  }

  virtual bool Guard(Tx &tx_box, Row* row, int col_id, bool write=true)
      override {
    // do nothing.
    return true;
  }

  virtual bool DoPrepare(txnid_t tx_id) override {
    return false;
  }

};

} // janus
