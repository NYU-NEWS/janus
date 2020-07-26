#pragma once
#include "deptran/rcc/commo.h"

namespace janus {

class PreAcceptQuorumEvent : public QuorumEvent {
 public:
  using QuorumEvent::QuorumEvent;
//  vector<shared_ptr<RccGraph>> graphs_;
  vector<shared_ptr<parent_set_t>> vec_parents_{};
  parid_t partition_id_{0};
};

class TroadCommo : public RccCommo {
 public:
  using RccCommo::RccCommo;
  void SendDispatch(vector<SimpleCommand>& cmd,
                    const function<void(int res,
                                        TxnOutput& output,
                                        RccGraph& graph)>&) override;
  void SendHandoutRo(SimpleCommand& cmd,
                     const function<void(int res,
                                         SimpleCommand& cmd,
                                         map<int, mdb::version_t>& vers)>&)
  override;

  void SendInquire(parid_t pid,
                   epoch_t epoch,
                   txnid_t tid,
                   const function<void(RccGraph& graph)>&) override;

  void BroadcastPreAccept(parid_t par_id,
                          txnid_t txn_id,
                          rank_t rank,
                          ballot_t ballot,
                          vector<SimpleCommand>& cmds,
                          shared_ptr<RccGraph> graph,
                          const function<void(int32_t, shared_ptr<RccGraph>)>& callback);

  shared_ptr<PreAcceptQuorumEvent>
  BroadcastPreAccept(parid_t par_id,
                     txnid_t txn_id,
                     rank_t rank,
                     ballot_t ballot,
                     vector<SimpleCommand>& cmds);

  shared_ptr<PreAcceptQuorumEvent>
  BroadcastPreAccept(parid_t par_id,
                     txnid_t txn_id,
                     rank_t rank,
                     ballot_t ballot,
                     vector<SimpleCommand>& cmds,
                     shared_ptr<RccGraph> graph);

  void BroadcastAccept(parid_t par_id,
                       txnid_t cmd_id,
                       ballot_t ballot,
                       shared_ptr<RccGraph> graph,
                       const function<void(int)>& callback);

  shared_ptr<QuorumEvent> BroadcastAccept(parid_t par_id,
                                          txnid_t cmd_id,
                                          rank_t rank,
                                          ballot_t ballot,
                                          parent_set_t& parents);

  shared_ptr<QuorumEvent> BroadcastAccept(parid_t par_id,
                                          txnid_t cmd_id,
                                          ballot_t ballot,
                                          shared_ptr<RccGraph> graph);

  void BroadcastCommit(
      parid_t,
      txnid_t cmd_id_,
      rank_t rank,
      bool need_validation,
      shared_ptr<RccGraph> graph,
      const function<void(int32_t, TxnOutput&)>& callback) override;

  shared_ptr<QuorumEvent> BroadcastCommit(
      parid_t,
      txnid_t cmd_id_,
      rank_t rank,
      bool need_validation,
      parent_set_t& parents);
  shared_ptr<QuorumEvent> CollectValidation(txid_t txid, set<parid_t> pars);
  shared_ptr<QuorumEvent> BroadcastValidation(CmdData& cmd_, int validation);

};

} // namespace
