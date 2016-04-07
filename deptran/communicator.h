//
// Created by lamont on 12/21/15.
//
#pragma once

#include <chrono>
#include "__dep__.h"
#include "constants.h"
#include "msg.h"
#include "config.h"

namespace rococo {

class Coordinator;
class ClassicProxy;

class Communicator {
 public:
  const int CONNECT_TIMEOUT_MS = 30*1000;
  rrr::PollMgr *rpc_poll_ = nullptr;
  map<siteid_t, rrr::Client *> rpc_clients_ = {};
  map<siteid_t, ClassicProxy *> rpc_proxies_ = {};
  map<parid_t, vector<std::pair<siteid_t,
                                ClassicProxy*>>> rpc_par_proxies_ = {};

//  vector<rrr::Client*> rpc_clients_ = {};
//  vector<RococoProxy*> rpc_proxies_ = {};

  Communicator(PollMgr* poll_mgr = nullptr);
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
  virtual ~Communicator();

  std::pair<siteid_t, ClassicProxy*> RandomProxyForPartition(parid_t
                                                             partition_id) const;
  std::pair<siteid_t, ClassicProxy*> LeaderProxyForPartition(parid_t) const;

  std::pair<siteid_t, ClassicProxy*> NearestProxyForPartition(parid_t) const;

  std::pair<int, ClassicProxy*> ConnectToSite(rococo::Config::SiteInfo &site,
                                              std::chrono::milliseconds timeout_ms);
};

} // namespace rococo
