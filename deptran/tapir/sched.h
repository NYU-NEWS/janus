#pragma once

#include "../classic/sched.h"

namespace rococo {

class SimpleCommand;
class TapirSched : public ClassicSched {
 public:
//  using Scheduler::Scheduler;
  TapirSched(): ClassicSched() {
    epoch_enabled_ = true;
  }

  int OnDispatch(const vector<SimpleCommand> &cmd,
                 int *res,
                 TxnOutput *output,
                 const function<void()> &callback) override;

  int OnFastAccept(cmdid_t cmd_id,
                   const vector<SimpleCommand>& txn_cmds,
                   int32_t* res,
                   const function<void()>& callback);

  int OnDecide(cmdid_t cmd_id,
               int32_t decision,
               const function<void()>& callback);
};

} // namespace rococo