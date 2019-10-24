
#pragma once
#include "deptran/rococo/scheduler.h"

namespace janus {

class RccGraph;
class JanusCommo;
class SchedulerJanus : public SchedulerRococo {
 public:
  using SchedulerRococo::SchedulerRococo;

  map<txnid_t, shared_ptr<TxRococo>> Aggregate(RccGraph& graph) override;

  virtual int OnPreAccept(txnid_t txnid,
                          rank_t rank,
                          const vector<SimpleCommand> &cmds,
                          shared_ptr<RccGraph> sp_graph,
                          shared_ptr<RccGraph> res_graph);

  void OnAccept(txnid_t txn_id,
                const ballot_t& ballot,
                shared_ptr<RccGraph> graph,
                int32_t* res);

  int OnCommit(txnid_t txn_id,
               rank_t rank,
               shared_ptr<RccGraph> sp_graph,
               TxnOutput *output) override;

  int OnInquire(epoch_t epoch,
                cmdid_t cmd_id,
                shared_ptr<RccGraph> graph) override;
  JanusCommo* commo();

};
} // namespace janus
