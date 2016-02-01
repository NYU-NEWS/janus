//
// Created by lamont on 12/21/15.
//
#pragma once

#include "__dep__.h"
#include "constants.h"
#include "msg.h"

namespace rococo {
class Coordinator;
class RococoProxy;

class Communicator {
 public:
  rrr::PollMgr *rpc_poll_ = nullptr;
  std::vector<rrr::Client *> vec_rpc_cli_ = {};
  std::vector<RococoProxy *> vec_rpc_proxy_ = {};

  Communicator(std::vector<std::string> &addrs);

  virtual void SendStart(SimpleCommand& cmd,
                         int32_t output_size,
                         std::function<void(Future *)> &callback) = 0;
  virtual void SendStart(SimpleCommand& cmd,
                         Coordinator *coo,
                         const std::function<void(rococo::StartReply&)> &) = 0;
  virtual void SendPrepare(parid_t gid,
                           txnid_t tid,
                           std::vector<int32_t> &sids,
                           const std::function<void(Future *fu)> &callback) = 0;
  virtual void SendCommit(parid_t pid,
                          txnid_t tid,
                          const std::function<void(Future *fu)> &callback) = 0;
  virtual void SendAbort(parid_t pid,
                         txnid_t tid,
                         const std::function<void(Future *fu)> &callback) = 0;
  virtual ~Communicator() {}
};

} // namespace rococo
