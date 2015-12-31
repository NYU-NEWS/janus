//
// Created by lamont on 12/22/15.
//
#pragma once
#include "msg.h"
#include "deptran/coordinator.h"
#include "./mdcc.h"

namespace mdcc {
using rococo::ThreePhaseCommunicator;
using rococo::Coordinator;
using rococo::StartReply;
using rococo::RequestHeader;

class MdccCommunicator: public ThreePhaseCommunicator {
 public:
  MdccCommunicator(vector<string> &addrs) {
    verify(addrs.size() > 0);
    poll_ = new PollMgr(1);
    for (auto& addr : addrs) {
      auto proxy = new SiteProxy(addr, poll_);
      proxies_.push_back(proxy);
    }
  }

  void SendStart(groupid_t gid,
                 RequestHeader &header,
                 map<int32_t, Value> &input,
                 int32_t output_size,
                 std::function<void(Future *fu)> &callback) override;

  void SendStart(parid_t par_id,
                 rococo::StartRequest &req,
                 Coordinator *coo,
                 std::function<void(rococo::StartReply&)> &callback) override;

  void SendPrepare(parid_t gid,
                   txnid_t tid,
                   std::vector<int32_t> &sids,
                   std::function<void(Future *fu)> &callback) override;

  void SendCommit(parid_t pid, txnid_t tid,
                  std::function<void(Future *fu)> &callback) override;

  void SendAbort(parid_t pid, txnid_t tid,
                 std::function<void(Future *fu)> &callback) override;

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
  std::vector<SiteProxy*> proxies_;
};
}

