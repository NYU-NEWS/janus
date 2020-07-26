
#pragma once
#include "../rcc/server.h"

namespace janus {

class RccGraph;
class JanusCommo;
class SchedulerJanus : public RccServer {
 public:
  using RccServer::RccServer;

  map<txnid_t, shared_ptr<RccTx>> Aggregate(RccGraph& graph) override;

  virtual int OnPreAccept(txnid_t txnid,
                          rank_t rank,
                          const vector<SimpleCommand> &cmds,
                          shared_ptr<RccGraph> sp_graph,
                          shared_ptr<RccGraph> res_graph);

  void OnAccept(txnid_t txn_id,
                int rank,
                const ballot_t& ballot,
                shared_ptr<RccGraph> graph,
                int32_t* res);

//  int OnCommit(txnid_t txn_id,
//               rank_t rank,
//               bool need_validation,
//               shared_ptr<RccGraph> sp_graph,
//               TxnOutput *output) override;

  JanusCommo* commo();

};
} // namespace janus
