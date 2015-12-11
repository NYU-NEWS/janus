#pragma once

#include "__dep__.h"
#include "constants.h"
#include "rcc/graph.h"
#include "rcc/graph_marshaler.h"
#include "rcc_rpc.h"
#include "msg.h"

namespace rococo {

class Coordinator;

class Commo {
 public:
  rrr::PollMgr *rpc_poll_ = nullptr;
  std::vector<rrr::Client *> vec_rpc_cli_;
  std::vector<RococoProxy *> vec_rpc_proxy_;

  Commo(std::vector<std::string> &addrs);

  void SendStart(groupid_t gid,
                 RequestHeader &header,
                 std::vector<Value> &input,
                 int32_t output_size,
                 std::function<void(Future *fu)> &callback);
  void SendStart(parid_t par_id, 
                 StartRequest &req, 
                 Coordinator *coo,
                 std::function<void(StartReply&)> &callback);
  void SendPrepare(parid_t gid,
                   txnid_t tid, 
                   std::vector<int32_t> &sids, 
                   std::function<void(Future *fu)> &callback);
  void SendCommit(parid_t pid, txnid_t tid,
                  std::function<void(Future *fu)> &callback);
  void SendAbort(parid_t pid, txnid_t tid,
                 std::function<void(Future *fu)> &callback);

  // for debug
  set<std::pair<txnid_t, parid_t>> phase_three_sent_;

  void ___LogSent(parid_t pid, txnid_t tid);
};
} // namespace rococo
