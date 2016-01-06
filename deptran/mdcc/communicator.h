//
// Created by lamont on 12/22/15.
//
#pragma once
#include "msg.h"
#include "deptran/coordinator.h"
#include "./mdcc.h"

namespace mdcc {
using rococo::Config;

class MdccCommunicator {
 public:
  MdccCommunicator(Config* config) {
    poll_ = new PollMgr(1);
    // todo move somewhere else
    auto& replica_groups = config->replica_groups_;
    for (auto& group : replica_groups) {
      auto partition_id = group.partition_id;
      proxy_by_partition_id.push_back(std::vector<SiteProxy*>());
      int datacenter_id = 0;
      for (auto& replica : group.replicas) {
        proxy_by_datacenter.push_back(std::vector<SiteProxy*>());
        auto proxy = new SiteProxy(replica.GetHostAddr(), poll_);
        proxy_by_datacenter[datacenter_id].push_back(proxy);
        proxy_by_partition_id[partition_id].push_back(proxy);
        datacenter_id++;
      }
    }
  }

  void SendStart(StartRequest& req,
                 std::function<void(StartResponse &)>& callback);

 protected:
  struct SiteProxy {
    const string& address;
    rrr::Client* client;
    MdccLeaderProxy* leader;
    MdccAcceptorProxy* acceptor;
    SiteProxy() = delete;
    SiteProxy(const string &addr, rrr::PollMgr* poll)
      : address(addr) {
      client = new rrr::Client(poll);
      Log::info("connect to site: %s", addr.c_str());
      auto ret = client->connect(addr.c_str());
      if (ret) {
        Log::fatal("connect to %s failed.\n", addr.c_str());
      }
      leader = new MdccLeaderProxy(client);
      acceptor = new MdccAcceptorProxy(client);
    }

    virtual ~SiteProxy() {
      delete leader;
      delete acceptor;
      client->close_and_release();
    }
  };

  PollMgr* poll_;

  std::vector<vector<SiteProxy*>> proxy_by_partition_id;
  std::vector<vector<SiteProxy*>> proxy_by_datacenter;
};
}

