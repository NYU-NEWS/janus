#pragma once

#include "../classic/scheduler.h"

namespace janus {

class SimpleCommand;
class SchedulerTapir : public SchedulerClassic {
 public:
//  using Scheduler::Scheduler;
  SchedulerTapir() : SchedulerClassic() {
    epoch_enabled_ = true;
  }

  virtual bool Guard(Tx &tx_box, Row *row, int col_id, bool write) override;

  int OnFastAccept(txid_t tx_id,
                   const vector<SimpleCommand> &txn_cmds);

  int OnDecide(txid_t tx_id,
               int32_t decision,
               const function<void()> &callback);

  virtual bool HandleConflicts(Tx &dtxn,
                               innid_t inn_id,
                               vector<string> &conflicts) override {
    verify(0);
  };

  virtual bool DispatchPiece(Tx& tx,
                             TxPieceData& cmd,
                             TxnOutput& ret_output) override {
    SchedulerClassic::DispatchPiece(tx, cmd, ret_output);
    ExecutePiece(tx, cmd, ret_output);
    return true;
  }

};

} // namespace janus
