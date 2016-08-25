#pragma once

#include "graph.h"
#include "dtxn.h"
#include "marshal-value.h"
#include "command.h"
#include "command_marshaler.h"
#include "../marshallable.h"

/**
 * This is NOT thread safe!!!
 */
namespace rococo {

typedef RccDTxn RccVertex;
typedef vector<RccVertex*> RccScc;

class EmptyGraph : public Marshallable {
 public:
  EmptyGraph() {rtti_ = Marshallable::Kind::EMPTY_GRAPH;};
  virtual Marshal& ToMarshal(Marshal& m) const {return m;};
  virtual Marshal& FromMarshal(Marshal& m) {return m;};
};

class RccSched;
class RccGraph : public Graph<RccVertex> {
 public:
//    Graph<PieInfo> pie_gra_;
//  Graph <TxnInfo> txn_gra_;
  RccSched* sched_{nullptr};
  parid_t partition_id_ = 0; // TODO
//  std::vector<rrr::Client *> rpc_clients_;
//  std::vector<RococoProxy *> rpc_proxies_;
//  std::vector<std::string> server_addrs_;

  RccGraph() : Graph<RccVertex>() {
    rtti_ = Marshallable::RCC_GRAPH;
  }

  virtual ~RccGraph() {
    // XXX hopefully some memory leak here does not hurt. :(
  }

  virtual std::shared_ptr<Marshallable>& ptr() override {
    data_ = shared_from_this();
    return data_;
  };

  /** on start_req */
  RccVertex* FindOrCreateRccVertex(txnid_t txn_id, RccSched* sched);
  void RemoveVertex(txnid_t txn_id);
  virtual void BuildEdgePointer(RccGraph &graph,
                                map<txnid_t, RccVertex*>& index);
  void RebuildEdgePointer(map<txnid_t, RccVertex*>& index);
  RccVertex* AggregateVertex(RccVertex *rhs_v);
  void UpgradeStatus(RccVertex* v, int8_t status);

  map<txnid_t, RccVertex*> Aggregate(epoch_t epoch, RccGraph& graph);
  void SelectGraphCmtUkn(RccVertex* vertex, RccGraph* new_graph);
  void SelectGraph(set<RccVertex*> vertexes, RccGraph* new_graph);
  RccScc& FindSCC(RccVertex *vertex) override;
  bool AllAncCmt(RccVertex *vertex);

  bool operator== (RccGraph& rhs) const;

  bool operator!= (RccGraph& rhs) const {
    // TODO
    return !(*this == rhs);
  }

  uint64_t MinItfrGraph(uint64_t tid,
                        RccGraph* gra_m,
                        bool quick = false,
                        int depth = -1);

  bool HasICycle(const RccScc& scc);


//  Marshal& ToMarshal(Marshal& m) const override;
//  Marshal& FromMarshal(Marshal& m) override;

};
} // namespace rcc
