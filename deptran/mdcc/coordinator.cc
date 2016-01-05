//
// Created by lamont on 12/18/15.
//
#include "coordinator.h"
#include "deptran/frame.h"

namespace mdcc {

void MdccCoordinator::do_one(TxnRequest &req) {
  Log_info("MdccCoord::do_one: type=%d", req.txn_type_);
  std::lock_guard<std::mutex> lock(this->mtx_);
  TxnChopper *ch = Frame().CreateChopper(req, txn_reg_);
  verify(txn_reg_ != nullptr);
  cmd_ = ch;
  cmd_id_ = this->next_txn_id();
  cleanup(); // In case of reuse.

}

}
