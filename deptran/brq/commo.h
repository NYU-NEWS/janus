#pragma once
#include "../rcc/commo.h"
#include "brq-common.h"

namespace rococo {

class BrqCommo: public RccCommo {
 public:
  using RccCommo::RccCommo;
  void SendDispatch(vector<SimpleCommand> &cmd,
                    const function<void(int res,
                                        TxnOutput& output,
                                        RccGraph &graph)> &);
  void SendHandoutRo(SimpleCommand &cmd,
                     const function<void(int res,
                                         SimpleCommand &cmd,
                                         map<int, mdb::version_t> &vers)> &)
  override;

  void SendFinish(parid_t pid,
                  txnid_t tid,
                  RccGraph &graph,
                  const function<void(TxnOutput&)>&) override;

  void SendInquire(parid_t pid,
                   epoch_t epoch,
                   txnid_t tid,
                   const function<void(RccGraph &graph)> &) override;

  void BroadcastPreAccept(parid_t par_id,
                          txnid_t txn_id,
                          ballot_t ballot,
                          vector<SimpleCommand>& cmds,
                          RccGraph& graph,
                          const function<void(int32_t, RccGraph*)> &callback);

  void BroadcastAccept(parid_t par_id,
                       txnid_t cmd_id,
                       ballot_t ballot,
                       RccGraph& graph,
                       const function<void(int)> &callback);

  void BroadcastCommit(parid_t,
                       txnid_t cmd_id_,
                       RccGraph& graph,
                       const function<void(int32_t, TxnOutput&)>
                       &callback);

  bool IsGraphOrphan(RccGraph& graph, txnid_t cmd_id);

};

} // namespace
