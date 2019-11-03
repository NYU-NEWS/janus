
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

class RccServer : public TxLogServer, public RccGraph {
 public:
  static map<txnid_t, int32_t> __debug_xxx_s;
  static std::recursive_mutex __debug_mutex_s;
  static void __DebugCheckParentSetSize(txnid_t tid, int32_t sz);
  AvgStat traversing_stat_{};

 public:
  set<shared_ptr<RccTx>> fridge_ = {};
  std::recursive_mutex mtx_{};
  std::time_t last_upgrade_time_{0};
  map<parid_t, int32_t> epoch_replies_{};
  bool in_upgrade_epoch_{false};
  const int EPOCH_DURATION = 5;

  RccServer();
  virtual ~RccServer();

  using RccGraph::Aggregate; // C++ has strange hiding rules
  virtual map<txnid_t, shared_ptr<RccTx>> Aggregate(RccGraph &graph);
  // override graph operations
  unordered_map<txnid_t, shared_ptr<RccTx>> &vertex_index() override {
    verify(!managing_memory_);
    return reinterpret_cast<
        std::unordered_map<txnid_t, shared_ptr<RccTx>> &>(dtxns_);
  };
  shared_ptr<RccTx> CreateV(txnid_t txn_id) override {
    auto sp_tx = CreateTx(txn_id);
    return dynamic_pointer_cast<RccTx>(sp_tx);
  }
  shared_ptr<RccTx> CreateV(RccTx &rhs) override {
    auto dtxn = dynamic_pointer_cast<RccTx>(CreateTx(rhs.id()));
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
    TxLogServer::partition_id_ = par_id;
    RccGraph::partition_id_ = par_id;
  }

  int OnDispatch(const vector<SimpleCommand> &cmd,
                 TxnOutput *output,
                 shared_ptr<RccGraph> graph);

  int OnCommit(cmdid_t cmd_id,
               rank_t rank,
               const RccGraph &graph,
               TxnOutput *output,
               const function<void()> &callback);

  virtual int OnInquire(epoch_t epoch,
                        txnid_t cmd_id,
                        shared_ptr<RccGraph> graph);

  virtual bool HandleConflicts(Tx &dtxn,
                               innid_t inn_id,
                               vector<string> &conflicts) override {
    verify(0);
  };

  void DestroyExecutor(txnid_t tid) override;

  void InquireAboutIfNeeded(RccTx &dtxn);
  void InquiredGraph(RccTx &dtxn, shared_ptr<RccGraph> graph);
  void InquireAck(cmdid_t cmd_id, RccGraph &graph);
  bool AllAncCmt(RccTx* v);
  void WaitUntilAllPredecessorsAtLeastCommitting(RccTx* v);
  void WaitUntilAllPredSccExecuted(const RccScc &);
  bool FullyDispatched(const RccScc &scc, rank_t r=RANK_UNDEFINED);
  bool IsExecuted(const RccScc &scc, rank_t r=RANK_UNDEFINED);
  void Decide(const RccScc &);
  bool HasICycle(const RccScc &scc);
  bool HasAbortedAncestor(const RccScc &scc);
  bool AllAncFns(const RccScc &);
  void Execute(const RccScc &);
  void Execute(RccTx &);
  void Abort(const RccScc &);
  virtual int OnCommit(txnid_t txn_id,
                       rank_t rank,
                       bool need_validation,
                       shared_ptr<RccGraph> sp_graph,
                       TxnOutput *output);
  /**
   *
   * @return validation result
   */
  int OnInquireValidation(txid_t tx_id);
  void OnNotifyGlobalValidation(txid_t tx_id, int validation_result);


  void __DebugExamineFridge();
  RccTx &__DebugFindAnOngoingAncestor(RccTx &vertex);
  void __DebugExamineGraphVerify(RccTx &v);
  RccCommo *commo();
};

} // namespace janus
