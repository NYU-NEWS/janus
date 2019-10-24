#pragma once
#include "../rococo/commo.h"

namespace janus {

class JanusCommo : public RccCommo {
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
                          ballot_t ballot,
                          vector<SimpleCommand>& cmds,
                          shared_ptr<RccGraph> graph,
                          const function<void(int32_t, shared_ptr<RccGraph>)>& callback);

  void BroadcastAccept(parid_t par_id,
                       txnid_t cmd_id,
                       ballot_t ballot,
                       shared_ptr<RccGraph> graph,
                       const function<void(int)>& callback);

  void BroadcastCommit(
      parid_t,
      txnid_t cmd_id_,
      rank_t rank,
      shared_ptr<RccGraph> graph,
      const function<void(int32_t, TxnOutput&)>& callback) override;


};

} // namespace
