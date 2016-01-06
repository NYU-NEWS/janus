//
// Created by lamont on 12/22/15.
//

#include "communicator.h"

namespace mdcc {
  void MdccCommunicator::SendStart(StartRequest& req,
                                   std::function<void(StartResponse&)>& callback) {
    Log::info("%s %ld", __FUNCTION__, req.txn_id);
  }
}
