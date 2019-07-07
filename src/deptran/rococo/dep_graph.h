#pragma once

#include "graph.h"
#include "tx.h"
#include "marshal-value.h"
#include "command.h"
#include "command_marshaler.h"
#include "../marshallable.h"

/**
 * This is NOT thread safe!!!
 */
namespace janus {

//typedef RccDTxn RccDTxn;
typedef vector<TxRococo*> RccScc;

class EmptyGraph : public Marshallable {
 public:
  EmptyGraph() : Marshallable(MarshallDeputy::EMPTY_GRAPH) {};
  virtual Marshal& ToMarshal(Marshal& m) const {return m;};
  virtual Marshal& FromMarshal(Marshal& m) {return m;};
};

class SchedulerRococo;
class RccGraph : public Graph<TxRococo> {
 public:
//    Graph<PieInfo> pie_gra_;
//  Graph <TxnInfo> txn_gra_;
  SchedulerRococo* sched_{nullptr};
  bool empty_{false};
  parid_t partition_id_ = 0; // TODO
//  std::vector<rrr::Client *> rpc_clients_;
//  std::vector<RococoProxy *> rpc_proxies_;
//  std::vector<std::string> server_addrs_;

  RccGraph() : Graph<TxRococo>() {
    kind_ = MarshallDeputy::RCC_GRAPH;
  }

  virtual ~RccGraph() {
    // XXX hopefully some memory leak here does not hurt. :(
  }

  /** on start_req */
  shared_ptr<TxRococo> FindOrCreateRccVertex(txnid_t txn_id,
                                            SchedulerRococo* sched);
  void RemoveVertex(txnid_t txn_id);
  void RebuildEdgePointer(map<txnid_t, shared_ptr<TxRococo>>& index);
  shared_ptr<TxRococo> AggregateVertex(shared_ptr<TxRococo> rhs_dtxn);
  void UpgradeStatus(TxRococo& v, int8_t status);

  virtual map<txnid_t, shared_ptr<TxRococo>> Aggregate(epoch_t epoch, RccGraph& graph);
  void SelectGraphCmtUkn(TxRococo& dtxn, shared_ptr<RccGraph> new_graph);
  void SelectGraph(set<shared_ptr<TxRococo>> vertexes, RccGraph* new_graph);
//  RccScc& FindSCC(RccDTxn *vertex) override;
  bool AllAncCmt(shared_ptr<TxRococo> vertex);

  bool operator== (RccGraph& rhs) const;

  bool operator!= (RccGraph& rhs) const {
    // TODO
    return !(*this == rhs);
  }

  uint64_t MinItfrGraph(TxRococo& dtxn,
                        shared_ptr<RccGraph> gra_m,
                        bool quick = false,
                        int depth = -1);

  bool HasICycle(const RccScc& scc);


//  Marshal& ToMarshal(Marshal& m) const override;
//  Marshal& FromMarshal(Marshal& m) override;

};
} // namespace janus
