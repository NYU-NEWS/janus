
#pragma once
#include "deptran/rococo/scheduler.h"

namespace janus {

class RccGraph;
class JanusCommo;
class SchedulerJanus : public SchedulerRococo {
 public:
  using SchedulerRococo::SchedulerRococo;

  map<txnid_t, shared_ptr<TxRococo>> Aggregate(RccGraph& graph) override;

  void OnPreAccept(txnid_t txnid,
                   const vector<SimpleCommand> &cmds,
                   RccGraph* graph,
                   int32_t *res,
                   shared_ptr<RccGraph> res_graph);


  void OnAccept(txnid_t txn_id,
                const ballot_t& ballot,
                shared_ptr<RccGraph> graph,
                int32_t* res);

//  void OnCommit(const txnid_t txn_id,
//                const RccGraph &graph,
//                int32_t *res,
//                TxnOutput *output,
//                const function<void()> &callback);

//  void OnCommitWoGraph(const txnid_t cmd_id,
//                       int32_t* res,
//                       TxnOutput* output,
//                       const function<void()>& callback);

  void OnCommit(txnid_t txn_id,
                RccGraph* graph,
                int32_t *res,
                TxnOutput *output,
                const function<void()> &callback) override;

  int OnInquire(epoch_t epoch,
                cmdid_t cmd_id,
                shared_ptr<RccGraph> graph,
                const function<void()> &callback) override;
  JanusCommo* commo();

};
} // namespace janus
