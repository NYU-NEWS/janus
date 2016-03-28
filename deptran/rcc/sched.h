
#pragma once

#include "../__dep__.h"
#include "../scheduler.h"
#include "graph.h"
#include "dep_graph.h"

namespace rococo {

class SimpleCommand;
class RccGraph;
class RccCommo;
class TxnInfo;
class WaitlistChecker;

class RccSched : public Scheduler {
 public:
  RccGraph *dep_graph_ = nullptr;
  RccCommo* commo_ = nullptr;
  WaitlistChecker* waitlist_checker_ = nullptr;
  list<RccVertex*> waitlist_ = {};
//  Vertex<TxnInfo> *v : wait_list_

  RccSched();
  virtual ~RccSched();

  virtual void SetPartitionId(parid_t par_id) {
    partition_id_ = par_id;
    dep_graph_->partition_id_ = par_id;
  }

  int OnHandoutRequest(const SimpleCommand &cmd,
                       rrr::i32 *res,
                       map<int32_t, Value> *output,
                       RccGraph* graph,
                       const function<void()>& callback);

  int OnFinishRequest(cmdid_t cmd_id,
                      const RccGraph& graph,
                      map<innid_t, map<int32_t, Value>> *output,
                      const function<void()>& callback);

  int OnInquireRequest(cmdid_t cmd_id,
                       RccGraph *graph,
                       const function<void()> &callback);

//  void to_decide(Vertex<TxnInfo> *v,
//                 rrr::DeferredReply *defer);

  void InquireAbout(Vertex<TxnInfo> *av);

  void CheckWaitlist();

  void InquireAck(RccGraph& graph);

  bool AllAncCmt(RccVertex *v);
  void Decide(const RccScc&);

  bool AllAncFns(const RccScc&);
  void Execute(const RccScc&);

  void __DebugExamineWaitlist();

  RccCommo* commo() {
    return commo_;
  }
};

} // namespace rococo