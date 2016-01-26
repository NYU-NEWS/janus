//
// Created by lamont on 1/11/16.
//
#include <deptran/dtxn.h>
#include <deptran/txn_reg.h>
#include "deptran/txn_chopper.h"
#include "deptran/frame.h"
#include "executor.h"

namespace mdcc {
  using rococo::TxnRequest;
  using rococo::Frame;
  using rococo::SimpleCommand;

  MdccExecutor::MdccExecutor(txnid_t txn_id, Scheduler* sched) : Executor(txn_id, sched) {
    Log_debug("mdcc executor created for txn_id %ld\n", txn_id);
  }

  i8 MdccExecutor::Start(uint32_t txn_type, const map<int32_t, Value> &inputs) {
    return SUCCESS;
  }

  void MdccExecutor::StartPiece(const SimpleCommand& cmd, int32_t* result) {
    Log_info("%s type: %d; piece_id: %d;", __FUNCTION__, cmd.type_, cmd.inn_id_);
    auto handler_pair = this->txn_reg_->get(cmd);
    auto& c = const_cast<SimpleCommand&>(cmd);
    handler_pair.txn_handler(this, dtxn_, c, result, c.output);
  }
}
