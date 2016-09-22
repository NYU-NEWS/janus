
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
  AvgStat traversing_stat_{};

  class Waitlist {
    set<RccDTxn*> waitlist_ = {};
    set<RccDTxn*> fridge_ = {};
  };

 public:
//  RccGraph *dep_graph_ = nullptr;
  WaitlistChecker* waitlist_checker_ = nullptr;
  set<RccDTxn*> waitlist_ = {};
  set<RccDTxn*> fridge_ = {};
  std::recursive_mutex mtx_{};
  std::time_t last_upgrade_time_{0};
  map<parid_t, int32_t> epoch_replies_{};
  bool in_upgrade_epoch_{false};
  const int EPOCH_DURATION = 5;

  RccSched();
  virtual ~RccSched();

  // override graph operations
  std::unordered_map<txnid_t, RccDTxn*>& vertex_index() override {
    verify(!managing_memory_);
    return reinterpret_cast<std::unordered_map<txnid_t, RccDTxn*>&>(dtxns_);
  };
  RccDTxn* CreateV(txnid_t txn_id) override {
    auto dtxn = (RccDTxn*) CreateDTxn(txn_id);
    return dtxn;
  }
  RccDTxn* CreateV(RccDTxn& rhs) override {
    auto dtxn = (RccDTxn*) CreateDTxn(rhs.id());
    if (rhs.epoch_ > 0) {
      dtxn->epoch_ = rhs.epoch_;
    }
    dtxn->partition_ = rhs.partition_;
    dtxn->status_ = rhs.status_;
    verify(dtxn->id() == rhs.tid_);
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

  virtual int OnInquire(epoch_t epoch,
                        txnid_t cmd_id,
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

  void InquireAboutIfNeeded(RccDTxn &dtxn);
  void AnswerIfInquired(RccDTxn &dtxn);
  void InquiredGraph(RccDTxn& dtxn, RccGraph* graph);
  void CheckWaitlist();
  void InquireAck(cmdid_t cmd_id, RccGraph& graph);
  void TriggerCheckAfterAggregation(RccGraph &graph);
  void AddChildrenIntoWaitlist(RccDTxn* v);
  bool AllAncCmt(RccDTxn *v);
  bool FullyDispatched(const RccScc& scc);
  void Decide(const RccScc&);
  bool HasICycle(const RccScc& scc);
  bool HasAbortedAncestor(const RccScc& scc);
  bool AllAncFns(const RccScc&);
  void Execute(const RccScc&);
  void Execute(RccDTxn&);
  void Abort(const RccScc&);

  void __DebugExamineFridge();
  RccDTxn* __DebugFindAnOngoingAncestor(RccDTxn* vertex);
  void __DebugExamineGraphVerify(RccDTxn *v);
  RccCommo* commo();
};

} // namespace rococo