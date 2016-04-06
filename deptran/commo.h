#pragma once

#include "__dep__.h"
#include "constants.h"
#include "rcc/graph.h"
#include "rcc/graph_marshaler.h"
#include "command.h"
#include "command_marshaler.h"
#include "rcc_rpc.h"
#include "msg.h"
#include "txn_chopper.h"
#include "communicator.h"


namespace rococo {

class Coordinator;


class RococoCommunicator : public Communicator {
 public:
  using Communicator::Communicator;
  void SendStart(SimpleCommand& cmd,
                 int32_t output_size,
                 std::function<void(Future *fu)> &callback);
  void SendDispatch(SimpleCommand &cmd,
                   Coordinator *coo,
                   const std::function<void(int res, ContainerCommand&)>&) ;
  void SendPrepare(parid_t gid,
                   txnid_t tid, 
                   std::vector<int32_t> &sids, 
                   const std::function<void(Future *fu)> &callback) ;
  void SendCommit(parid_t pid,
                  txnid_t tid,
                  const std::function<void(Future *fu)> &callback) ;
  void SendAbort(parid_t pid,
                 txnid_t tid,
                 const std::function<void(Future *fu)> &callback) ;

  // for debug
  set<std::pair<txnid_t, parid_t>> phase_three_sent_ = {};

  void ___LogSent(parid_t pid, txnid_t tid);
};
} // namespace rococo
