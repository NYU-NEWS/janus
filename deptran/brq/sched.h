
#pragma once
#include "../rcc/sched.h"

namespace rococo {
class RccGraph;
class BrqCommo;
class BrqSched : public RccSched {
 public:
  using RccSched::RccSched;

  map<txnid_t, RccDTxn*> Aggregate(RccGraph& graph);

  void OnPreAccept(const txnid_t txnid,
                   const vector<SimpleCommand> &cmds,
                   const RccGraph &graph,
                   int32_t *res,
                   RccGraph *res_graph,
                   function<void()> callback);

  void OnPreAcceptWoGraph(const txnid_t txnid,
                          const vector<SimpleCommand> &cmds,
                          int32_t *res,
                          RccGraph *res_graph,
                          function<void()> callback);

  void OnAccept(const txnid_t txn_id,
                const ballot_t& ballot,
                const RccGraph& graph,
                int32_t* res,
                function<void()> callback);

//  void OnCommit(const txnid_t txn_id,
//                const RccGraph &graph,
//                int32_t *res,
//                TxnOutput *output,
//                const function<void()> &callback);

  void OnCommit(const txnid_t txn_id,
                RccGraph* graph,
                int32_t *res,
                TxnOutput *output,
                const function<void()> &callback);

//  void OnCommitWoGraph(const txnid_t cmd_id,
//                       int32_t* res,
//                       TxnOutput* output,
//                       const function<void()>& callback);

  int OnInquire(epoch_t epoch,
                cmdid_t cmd_id,
                RccGraph *graph,
                const function<void()> &callback) override;
  BrqCommo* commo();

};
} // namespace rococo