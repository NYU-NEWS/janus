#pragma once

#include "deptran/classic/scheduler.h"

namespace janus {

class SimpleCommand;
class SchedulerTapir : public SchedulerClassic {
 public:
//  using Scheduler::Scheduler;
  SchedulerTapir() : SchedulerClassic() {
    epoch_enabled_ = true;
  }

  virtual bool Guard(Tx &tx_box, Row *row, int col_id, bool write) override;

  bool OnDispatch(TxPieceData &piece_data,
                  TxnOutput &ret_output) override;

  int OnFastAccept(txid_t tx_id,
                   const vector<SimpleCommand> &txn_cmds);

  int OnDecide(txid_t tx_id,
               int32_t decision,
               const function<void()> &callback);

  virtual bool HandleConflicts(Tx &dtxn,
                               innid_t inn_id,
                               vector<string> &conflicts) {
    verify(0);
  };

};

} // namespace janus