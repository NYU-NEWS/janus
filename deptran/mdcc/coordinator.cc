//
// Created by lamont on 12/18/15.
//
#include "coordinator.h"
#include "deptran/frame.h"

namespace mdcc {
  void MdccCoordinator::do_one(TxnRequest &req) {
    //std::lock_guard<std::mutex> lock(this->mtx_);
    Log_info("MdccCoord::do_one: type=%d", req.txn_type_);

    mdcc::StartRequest request;
    std::function<void(StartResponse&)> callback = [this, request](StartResponse& reply) {
      Log_debug("finished executing request %ld", request.txn_id);
    };

    request.txn_type = req.txn_type_;
    request.txn_id = this->NextTxnId();
    request.inputs = req.input_;
    Log_debug("send out start request %ld, cmd_id: %lx",
              n_start_, request.txn_id);
    this->communicator_->SendStart(request, callback);
  }

  i64 MdccCoordinator::NextTxnId() {
      return 0;
  }
}
