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
  map<siteid_t, rrr::Client *> rpc_clients_ = {};
  map<siteid_t, RococoProxy *> rpc_proxies_ = {};
  map<parid_t, vector<RococoProxy*>> rpc_par_proxies_ = {};

//  vector<rrr::Client*> rpc_clients_ = {};
//  vector<RococoProxy*> rpc_proxies_ = {};

  Communicator();
//
//  virtual void SendStart(SimpleCommand& cmd,
//                         int32_t output_size,
//                         std::function<void(Future *)> &callback) = 0;
//  virtual void SendStart(SimpleCommand& cmd,
//                         Coordinator *coo,
//                         const std::function<void(rococo::StartReply&)> &) = 0;
//  virtual void SendPrepare(parid_t gid,
//                           txnid_t tid,
//                           std::vector<int32_t> &sids,
//                           const std::function<void(Future *fu)> &callback) = 0;
//  virtual void SendCommit(parid_t pid,
//                          txnid_t tid,
//                          const std::function<void(Future *fu)> &callback) = 0;
//  virtual void SendAbort(parid_t pid,
//                         txnid_t tid,
//                         const std::function<void(Future *fu)> &callback) = 0;
  virtual ~Communicator() {}
};

} // namespace rococo
