//
// Created by lamont on 12/21/15.
//
#pragma once

#include "deptran/__dep__.h"
#include "constants.h"
#include "msg.h"

namespace rococo {
class Coordinator;

class ThreePhaseCommunicator {
 public:
  virtual void SendStart(groupid_t gid,
                         RequestHeader &header,
                         map<int32_t, Value> &input,
                         int32_t output_size,
                         std::function<void(Future *)> &callback) = 0;
  virtual void SendStart(parid_t par_id,
                         StartRequest &req,
                         Coordinator *coo,
                         std::function<void(StartReply&)> &callback) = 0;
  virtual void SendPrepare(parid_t gid,
                           txnid_t tid,
                           std::vector<int32_t> &sids,
                           std::function<void(Future *fu)> &callback) = 0;

  virtual void SendCommit(parid_t pid, txnid_t tid,
                          std::function<void(Future *fu)> &callback) = 0;
  virtual void SendAbort(parid_t pid, txnid_t tid,
                         std::function<void(Future *fu)> &callback) = 0;
};

}
