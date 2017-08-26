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

  bool OnDispatch(TxPieceData& piece_data,
                  TxnOutput& ret_output) override;

  int OnFastAccept(cmdid_t cmd_id,
                   const vector<SimpleCommand> &txn_cmds,
                   int32_t *res,
                   const function<void()> &callback);

  int OnDecide(cmdid_t cmd_id,
               int32_t decision,
               const function<void()> &callback);

  virtual bool HandleConflicts(TxBox &dtxn,
                               innid_t inn_id,
                               vector<string> &conflicts) {
    verify(0);
  };

};

} // namespace janus