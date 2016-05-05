
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
  WaitlistChecker* waitlist_checker_ = nullptr;
  set<RccVertex*> waitlist_ = {};
  set<RccVertex*> fridge_ = {};
  std::recursive_mutex mtx_{};
//  Vertex<TxnInfo> *v : wait_list_

  RccSched();
  virtual ~RccSched();

  DTxn* GetOrCreateDTxn(txnid_t tid, bool ro = false) override ;

  virtual void SetPartitionId(parid_t par_id) {
    partition_id_ = par_id;
    dep_graph_->partition_id_ = par_id;
  }

  int OnDispatch(const vector<SimpleCommand> &cmd,
                 rrr::i32 *res,
                 TxnOutput* output,
                 RccGraph *graph,
                 const function<void()> &callback);

  int OnCommit(cmdid_t cmd_id,
               const RccGraph &graph,
               TxnOutput* output,
               const function<void()> &callback);

  virtual int OnInquire(cmdid_t cmd_id,
                        RccGraph *graph,
                        const function<void()> &callback);

//  void to_decide(Vertex<TxnInfo> *v,
//                 rrr::DeferredReply *defer);

  void InquireAbout(Vertex<TxnInfo> *av);
  void InquireAboutIfNeeded(TxnInfo &info);
  void AnswerIfInquired(TxnInfo &tinfo);
  void CheckWaitlist();
  void InquireAck(cmdid_t cmd_id, RccGraph& graph);
  void TriggerCheckAfterAggregation(RccGraph &graph);
  bool AllAncCmt(RccVertex *v);
  void Decide(const RccScc&);
  bool HasICycle(const RccScc& scc);
  bool HasAbortedAncestor(const RccScc& scc);
  bool AllAncFns(const RccScc&);
  void Execute(const RccScc&);
  void Execute(TxnInfo&);
  void Abort(const RccScc&);

  void __DebugExamineFridge();
  RccVertex* __DebugFindAnOngoingAncestor(RccVertex* vertex);
  void __DebugExamineGraphVerify(RccVertex *v);
  RccCommo* commo();
};

} // namespace rococo