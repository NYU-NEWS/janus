
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

class RccSched : public Scheduler, public RccGraph {
 public:
  static map<txnid_t, int32_t> __debug_xxx_s;
  static std::recursive_mutex __debug_mutex_s;
  static void __DebugCheckParentSetSize(txnid_t tid, int32_t sz);

  class Waitlist {
    set<RccVertex*> waitlist_ = {};
    set<RccVertex*> fridge_ = {};
  };

 public:
//  RccGraph *dep_graph_ = nullptr;
  WaitlistChecker* waitlist_checker_ = nullptr;
  set<RccVertex*> waitlist_ = {};
  set<RccVertex*> fridge_ = {};
  std::recursive_mutex mtx_{};
  std::time_t last_upgrade_time_{0};
  map<parid_t, int32_t> epoch_replies_{};
  bool in_upgrade_epoch_{false};
  const int EPOCH_DURATION = 5;

  RccSched();
  virtual ~RccSched();

  // override graph operations
  RccDTxn* FindV(txnid_t txn_id) {
    auto dtxn = (RccDTxn*) GetDTxn(txn_id);
    verify(dtxn != nullptr);
    return dtxn;
  }
  RccDTxn* FindOrCreateV(txnid_t txn_id) {
    auto dtxn = (RccDTxn*) GetOrCreateDTxn(txn_id);
    return dtxn;
  }


  DTxn* GetOrCreateDTxn(txnid_t tid, bool ro = false) override ;

  virtual void SetPartitionId(parid_t par_id) {
    Scheduler::partition_id_ = par_id;
    RccGraph::partition_id_ = par_id;
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
//  DTxn* CreateDTxn(txnid_t tid, bool ro) override {
//    verify(0);
//  }
//
//  DTxn* GetDTxn(txnid_t tid) override {
//    verify(0);
//  }

  void DestroyExecutor(txnid_t tid) override;


  void InquireAbout(RccVertex *av);
  void InquireAboutIfNeeded(RccDTxn &dtxn);
  void AnswerIfInquired(RccDTxn &tinfo);
  void CheckWaitlist();
  void InquireAck(cmdid_t cmd_id, RccGraph& graph);
  void TriggerCheckAfterAggregation(RccGraph &graph);
  void AddChildrenIntoWaitlist(RccVertex* v);
  bool AllAncCmt(RccVertex *v);
  bool FullyDispatched(const RccScc& scc);
  void Decide(const RccScc&);
  bool HasICycle(const RccScc& scc);
  bool HasAbortedAncestor(const RccScc& scc);
  bool AllAncFns(const RccScc&);
  void Execute(const RccScc&);
  void Execute(RccDTxn&);
  void Abort(const RccScc&);

  void __DebugExamineFridge();
  RccVertex* __DebugFindAnOngoingAncestor(RccVertex* vertex);
  void __DebugExamineGraphVerify(RccVertex *v);
  RccCommo* commo();
};

} // namespace rococo