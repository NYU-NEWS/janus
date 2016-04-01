
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
  std::recursive_mutex mtx_;
//  Vertex<TxnInfo> *v : wait_list_

  RccSched();
  virtual ~RccSched();

  virtual void SetPartitionId(parid_t par_id) {
    partition_id_ = par_id;
    dep_graph_->partition_id_ = par_id;
  }

  int OnDispatch(const SimpleCommand &cmd,
                 rrr::i32 *res,
                 map<int32_t, Value> *output,
                 RccGraph *graph,
                 const function<void()> &callback);

  int OnCommit(cmdid_t cmd_id,
               const RccGraph &graph,
               TxnOutput* output,
               const function<void()> &callback);

  int OnInquire(cmdid_t cmd_id,
                RccGraph *graph,
                const function<void()> &callback);

//  void to_decide(Vertex<TxnInfo> *v,
//                 rrr::DeferredReply *defer);

  void InquireAbout(Vertex<TxnInfo> *av);

  void CheckWaitlist();

  void InquireAck(cmdid_t cmd_id, RccGraph& graph);

  bool AllAncCmt(RccVertex *v);
  void Decide(const RccScc&);

  bool AllAncFns(const RccScc&);
  void Execute(const RccScc&);

  void __DebugExamineWaitlist();
  RccVertex* __DebugFindAnOngoingAncestor(RccVertex* vertex);
  RccCommo* commo();
};

} // namespace rococo