
#pragma once

#include "../__dep__.h"
#include "../scheduler.h"
#include "graph.h"
#include "dep_graph.h"

namespace janus {

class SimpleCommand;
class RccGraph;
class RccCommo;
class TxnInfo;
class WaitlistChecker;

class SchedulerRococo : public Scheduler, public RccGraph {
 public:
  static map<txnid_t, int32_t> __debug_xxx_s;
  static std::recursive_mutex __debug_mutex_s;
  static void __DebugCheckParentSetSize(txnid_t tid, int32_t sz);
  AvgStat traversing_stat_{};

  class Waitlist {
    set<TxRococo *> waitlist_ = {};
    set<TxRococo *> fridge_ = {};
  };

 public:
//  RccGraph *dep_graph_ = nullptr;
  WaitlistChecker *waitlist_checker_ = nullptr;
  set<TxRococo *> waitlist_ = {};
  set<shared_ptr<TxRococo>> fridge_ = {};
  std::recursive_mutex mtx_{};
  std::time_t last_upgrade_time_{0};
  map<parid_t, int32_t> epoch_replies_{};
  bool in_upgrade_epoch_{false};
  const int EPOCH_DURATION = 5;

  SchedulerRococo();
  virtual ~SchedulerRococo();

  using RccGraph::Aggregate; // C++ has strange hiding rules
  virtual map<txnid_t, shared_ptr<TxRococo>> Aggregate(RccGraph &graph);
  // override graph operations
  unordered_map<txnid_t, shared_ptr<TxRococo>> &vertex_index() override {
    verify(!managing_memory_);
    return reinterpret_cast<
        std::unordered_map<txnid_t, shared_ptr<TxRococo>> &>(dtxns_);
  };
  shared_ptr<TxRococo> CreateV(txnid_t txn_id) override {
    auto sp_tx = CreateTx(txn_id);
    return dynamic_pointer_cast<TxRococo>(sp_tx);
  }
  shared_ptr<TxRococo> CreateV(TxRococo &rhs) override {
    auto dtxn = dynamic_pointer_cast<TxRococo>(CreateTx(rhs.id()));
    if (rhs.epoch_ > 0) {
      dtxn->epoch_ = rhs.epoch_;
    }
    dtxn->partition_ = rhs.partition_;
    dtxn->status_ = rhs.status_;
    verify(dtxn->id() == rhs.tid_);
    return dtxn;
  }
  shared_ptr<Tx> GetOrCreateTx(txnid_t tid, bool ro = false) override;

  virtual void SetPartitionId(parid_t par_id) override {
    Scheduler::partition_id_ = par_id;
    RccGraph::partition_id_ = par_id;
  }

  int OnDispatch(const vector<SimpleCommand> &cmd,
                 rrr::i32 *res,
                 TxnOutput *output,
                 shared_ptr<RccGraph> graph);

  int OnCommit(cmdid_t cmd_id,
               const RccGraph &graph,
               TxnOutput *output,
               const function<void()> &callback);

  virtual int OnInquire(epoch_t epoch,
                        txnid_t cmd_id,
                        shared_ptr<RccGraph> graph,
                        const function<void()> &callback);

  virtual bool HandleConflicts(Tx &dtxn,
                               innid_t inn_id,
                               vector<string> &conflicts) override {
    verify(0);
  };

  void DestroyExecutor(txnid_t tid) override;

  void InquireAboutIfNeeded(TxRococo &dtxn);
  void AnswerIfInquired(TxRococo &dtxn);
  void InquiredGraph(TxRococo &dtxn, shared_ptr<RccGraph> graph);
  void CheckWaitlist();
  void InquireAck(cmdid_t cmd_id, RccGraph &graph);
  void TriggerCheckAfterAggregation(RccGraph &graph);
  void AddChildrenIntoWaitlist(TxRococo *v);
  bool AllAncCmt(TxRococo &v);
  bool FullyDispatched(const RccScc &scc);
  void Decide(const RccScc &);
  bool HasICycle(const RccScc &scc);
  bool HasAbortedAncestor(const RccScc &scc);
  bool AllAncFns(const RccScc &);
  void Execute(const RccScc &);
  void Execute(TxRococo &);
  void Abort(const RccScc &);
  virtual void OnCommit(txnid_t txn_id,
                        RccGraph *graph,
                        int32_t *res,
                        TxnOutput *output,
                        const function<void()> &callback);

  void __DebugExamineFridge();
  TxRococo &__DebugFindAnOngoingAncestor(TxRococo &vertex);
  void __DebugExamineGraphVerify(TxRococo &v);
  RccCommo *commo();
};

} // namespace janus
