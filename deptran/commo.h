#pragma once

#include "__dep__.h"
#include "constants.h"
#include "rcc/graph.h"
#include "rcc/graph_marshaler.h"
#include "command.h"
#include "command_marshaler.h"
#include "rcc_rpc.h"
#include "msg.h"
#include "deptran/three_phase/communicator.h"

namespace rococo {

class Coordinator;


class RococoCommunicator : public ThreePhaseCommunicator {
 public:
  rrr::PollMgr *rpc_poll_ = nullptr;
  std::vector<rrr::Client *> vec_rpc_cli_;
  std::vector<RococoProxy *> vec_rpc_proxy_;

  RococoCommunicator(std::vector<std::string> &addrs);

  void SendStart(SimpleCommand& cmd,
                 int32_t output_size,
                 std::function<void(Future *fu)> &callback) override;
  void SendStart(SimpleCommand& cmd,
                 Coordinator *coo,
                 const std::function<void(StartReply&)> &) override;
  void SendPrepare(parid_t gid,
                   txnid_t tid, 
                   std::vector<int32_t> &sids, 
                   const std::function<void(Future *fu)> &callback) override;
  void SendCommit(parid_t pid,
                  txnid_t tid,
                  const std::function<void(Future *fu)> &callback) override;
  void SendAbort(parid_t pid,
                 txnid_t tid,
                 const std::function<void(Future *fu)> &callback) override;

  // for debug
  set<std::pair<txnid_t, parid_t>> phase_three_sent_;

  void ___LogSent(parid_t pid, txnid_t tid);
};
} // namespace rococo
