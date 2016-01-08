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
  MdccCommunicator() = delete;
  MdccCommunicator(Config* config, uint32_t site_id) :
    config_(config), poll_(new PollMgr(1)), site_info_(config->SiteById(site_id)) {
    for (auto& site_info : config->GetMyServers()) {
      this->site_proxies_.push_back(new SiteProxy(site_info, poll_));
    }
  }

  virtual ~MdccCommunicator() {
    for (auto proxy : this->site_proxies_) {
      delete proxy;
    }
  }

  void SendStart(StartRequest& req,
                 std::function<void(StartResponse &)>& callback);

 protected:
  struct SiteProxy {
    const Config::SiteInfo& site_info;
    rrr::Client* rpc_client;
    MdccLeaderProxy* leader;
    MdccAcceptorProxy* acceptor;
    MdccClientProxy* client;
    SiteProxy() = delete;
    SiteProxy(Config::SiteInfo& site_info, rrr::PollMgr* poll) : site_info(site_info) {
      std::string addr = site_info.GetHostAddr();
      rpc_client = new rrr::Client(poll);
      Log::info("connect to site: %s", addr.c_str());
      auto ret = rpc_client->connect(addr.c_str());
      if (ret) {
        Log::fatal("connect to %s failed.\n", addr.c_str());
      }
      leader = new MdccLeaderProxy(rpc_client);
      acceptor = new MdccAcceptorProxy(rpc_client);
      client = new MdccClientProxy(rpc_client);
    }

    virtual ~SiteProxy() {
      delete leader;
      delete acceptor;
      rpc_client->close_and_release();
    }
  };

  Config* config_;
  PollMgr* poll_;
  const Config::SiteInfo& site_info_; // my site_id
  std::vector<SiteProxy*> site_proxies_;

private:
  const Config::SiteInfo* ChooseRandomSite(const vector<Config::SiteInfo>& sites);
};
}

