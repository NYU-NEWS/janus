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
typedef vector<RccTx*> RccScc;

class EmptyGraph : public Marshallable {
 public:
  EmptyGraph() : Marshallable(MarshallDeputy::EMPTY_GRAPH) {};
  virtual Marshal& ToMarshal(Marshal& m) const {return m;};
  virtual Marshal& FromMarshal(Marshal& m) {return m;};
};

class RccServer;
class RccGraph : public Graph<RccTx> {
 public:
//    Graph<PieInfo> pie_gra_;
//  Graph <TxnInfo> txn_gra_;
  RccServer* sched_{nullptr};
  bool empty_{false};
//  parid_t partition_id_ = 0; // TODO
//  std::vector<rrr::Client *> rpc_clients_;
//  std::vector<RococoProxy *> rpc_proxies_;
//  std::vector<std::string> server_addrs_;

  RccGraph() : Graph<RccTx>() {
    kind_ = MarshallDeputy::RCC_GRAPH;
  }

  virtual ~RccGraph() {
    // XXX hopefully some memory leak here does not hurt. :(
  }

  /** on start_req */
  shared_ptr<RccTx> FindOrCreateRccVertex(txnid_t txn_id,
                                          RccServer* sched);
  void RemoveVertex(txnid_t txn_id);
  void RebuildEdgePointer(map<txnid_t, shared_ptr<RccTx>>& index);
  shared_ptr<RccTx> AggregateVertex(shared_ptr<RccTx> rhs_dtxn);
  void UpgradeStatus(RccTx& v, int rank, int8_t status);

  virtual map<txnid_t, shared_ptr<RccTx>> Aggregate(epoch_t epoch, RccGraph& graph);
  void SelectGraphCmtUkn(RccTx& dtxn, shared_ptr<RccGraph> new_graph);
  void SelectGraph(set<shared_ptr<RccTx>> vertexes, RccGraph* new_graph);
//  RccScc& FindSCC(RccDTxn *vertex) override;
  bool AllAncCmt(shared_ptr<RccTx> vertex);

  bool operator== (RccGraph& rhs) const;

  bool operator!= (RccGraph& rhs) const {
    // TODO
    return !(*this == rhs);
  }

  uint64_t MinItfrGraph(RccTx& dtxn,
                        shared_ptr<RccGraph> gra_m,
                        bool quick = false,
                        int depth = -1);

  bool HasICycle(const RccScc& scc);


//  Marshal& ToMarshal(Marshal& m) const override;
//  Marshal& FromMarshal(Marshal& m) override;

};
} // namespace janus
