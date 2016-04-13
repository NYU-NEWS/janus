//
// Created by lamont on 12/22/15.
//
#pragma once
#include "../txn_chopper.h"
#include "msg.h"
#include "deptran/coordinator.h"
#include "deptran/mdcc/Ballot.h"
#include "option.h"
#include "mdcc_rpc.h"

namespace mdcc {
using rococo::Config;

template<class T>
using Callback = std::function<void(const T&)>;

class MdccCommunicator {
 public:
  MdccCommunicator() = delete;
  MdccCommunicator(Config* config, uint32_t site_id) :
    config_(config), poll_(new PollMgr(1)), site_info_(config->SiteById(site_id)) {
    for (auto& site_info : config->sites_) {
      this->site_proxies_.push_back(new SiteProxy(site_info, poll_));
    }
  }

  virtual ~MdccCommunicator() {
    for (auto proxy : this->site_proxies_) {
      delete proxy;
    }
  }

  void SendStart(StartRequest& req,
                 Callback<StartResponse>& callback);

  void SendStartPiece(const rococo::SimpleCommand &cmd, rrr::FutureAttr *future);
  void SendProposal(Ballot ballot, txnid_t txn_id, const rococo::SimpleCommand &cmd,
                    OptionSet *options);
  void SendPhase2a(Phase2aRequest req);
  void SendPhase2b(const Phase2bRequest &req);

  void SendVisibility(txnid_t txn_id, bool accept);

protected:
  std::mutex mtx_;

  struct SiteProxy {
    const Config::SiteInfo& site_info;
    rrr::Client* rpc_client;
    MdccLeaderProxy* leader;
    MdccAcceptorProxy* acceptor;
    MdccClientProxy* client;
    MdccLearnerProxy* learner;

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
      learner = new MdccLearnerProxy(rpc_client);
    }

    virtual ~SiteProxy() {
      delete leader;
      delete acceptor;
      delete learner;
      rpc_client->close_and_release();
    }
  };

  Config* config_ = nullptr;
  PollMgr* poll_ = nullptr;
  const Config::SiteInfo& site_info_;
  std::vector<SiteProxy*> site_proxies_;

private:
  SiteProxy* RandomSiteProxy(const vector<Config::SiteInfo> &sites) const;
  SiteProxy* ClosestSiteProxy(uint32_t partition_id) const;
  std::vector<SiteProxy*> AllSiteProxies(parid_t partition_id) const;
  SiteProxy* LeaderSiteProxy(OptionSet *option_set, parid_t partition_id) const;
};
}

