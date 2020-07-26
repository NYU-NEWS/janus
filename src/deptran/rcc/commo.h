#pragma once
#include "../__dep__.h"
#include "../communicator.h"

namespace janus {
class SimpleCommand;
class RccGraph;
class RccCommo : public Communicator {
 public:
  using Communicator::Communicator;
  virtual void SendDispatch(
      vector<SimpleCommand>& cmd,
      const function<void(int res, TxnOutput& cmd, RccGraph& graph)>&);

  virtual void SendHandoutRo(
      SimpleCommand& cmd,
      const function<void(int res,
                          SimpleCommand& cmd,
                          map<int, mdb::version_t>& vers)>&);

  virtual void SendFinish(
      parid_t pid,
      txnid_t tid,
      shared_ptr<RccGraph> graph,
      const function<void(map<innid_t, map<int32_t, Value>>& output)>&);

  virtual void SendInquire(parid_t pid,
                           epoch_t epoch,
                           txnid_t tid,
                           const function<void(RccGraph& graph)>&);

  shared_ptr<map<txid_t, parent_set_t>>
  Inquire(parid_t pid, txnid_t tid, rank_t rank);

  virtual void BroadcastCommit(
                       parid_t,
                       txnid_t cmd_id_,
                       rank_t rank,
                       bool need_validation,
                       shared_ptr<RccGraph> graph,
                       const function<void(int32_t, TxnOutput&)>& callback);
  bool IsGraphOrphan(RccGraph& graph, txnid_t cmd_id);
  void BroadcastValidation(txid_t, set<parid_t>, int validation);
};

} // namespace janus
