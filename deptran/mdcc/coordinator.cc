//
// Created by lamont on 12/18/15.
//
#include "coordinator.h"
#include "deptran/frame.h"

namespace mdcc {
  void MdccCoordinator::do_one(TxnRequest &req) {
    Log_info("MdccCoord::do_one: type=%d", req.txn_type_);

    mdcc::StartRequest request;
    request.txn_type = req.txn_type_;
    request.txn_id = NextTxnId();
    request.inputs = req.input_;

    Callback<StartResponse> callback = [this, request, req](const StartResponse& reply) {
      TxnReply txn_reply;
      txn_reply.res_ = reply.result;
      if (reply.result==SUCCESS) {
        Log_debug("transaction %ld success!", request.txn_id);
        req.callback_(txn_reply);
        // TODO: report success
      } else {
        Log_debug("transaction %ld failed: %d", request.txn_id, reply.result);
        req.callback_(txn_reply);
        // TODO: report failure
      }
    };

    n_start_++;
    Log_debug("send out start request %ld, cmd_id: %lx",
              n_start_, request.txn_id);
    communicator_->SendStart(request, callback);
  }

  uint64_t MdccCoordinator::NextTxnId() {
    auto id = txn_id_counter_.fetch_add(1);
    return ((uint64_t)site_id_ << 32) | id;
  }
}
