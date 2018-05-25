#pragma once

#include "../classic/scheduler.h"

namespace janus {

class SchedulerNone: public SchedulerClassic {
 using SchedulerClassic::SchedulerClassic;
 public:

  virtual bool Dispatch(txid_t tx_id,
                        shared_ptr<Marshallable> cmd,
                        TxnOutput& ret_output) override {
    SchedulerClassic::Dispatch(tx_id, cmd, ret_output);
    auto tx = GetTx(tx_id);
    Execute(*tx, ret_output);
    return true;
  }

  virtual bool Guard(Tx &tx_box, Row* row, int col_id, bool write=true)
      override {
    // do nothing.
    return true;
  }

};

} // janus