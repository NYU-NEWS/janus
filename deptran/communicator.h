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

typedef std::pair<siteid_t, ClassicProxy*> SiteProxyPair;

class Communicator {
 public:
  const int CONNECT_TIMEOUT_MS = 120*1000;
  const int CONNECT_SLEEP_MS = 1000;
  rrr::PollMgr *rpc_poll_ = nullptr;
  locid_t loc_id_ = -1;
  map<siteid_t, rrr::Client *> rpc_clients_ = {};
  map<siteid_t, ClassicProxy *> rpc_proxies_ = {};
  map<parid_t, vector<SiteProxyPair>> rpc_par_proxies_ = {};
  map<parid_t, SiteProxyPair> leader_cache_ = {};

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

  SiteProxyPair RandomProxyForPartition(parid_t partition_id) const;
  SiteProxyPair LeaderProxyForPartition(parid_t) const;
  SiteProxyPair NearestProxyForPartition(parid_t) const;
  std::pair<int, ClassicProxy*> ConnectToSite(rococo::Config::SiteInfo &site, std::chrono::milliseconds timeout_ms);
};

} // namespace rococo
