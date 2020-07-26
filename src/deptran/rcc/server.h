
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
  map<txid_t, pair<RccTx::TraverseStatus, RccTx*>> __debug_search_status_i_{};
  map<txid_t, pair<RccTx::TraverseStatus, RccTx*>> __debug_search_status_d_{};
  map<txid_t, pair<RccTx::TraverseStatus, RccTx*>>& __debug_search_status(rank_t rank) {
    verify(rank == RANK_I || rank == RANK_D);
    if (rank == RANK_I) {
      return __debug_search_status_i_;
    } else {
      return __debug_search_status_d_;
    }
  };

  set<shared_ptr<RccTx>> fridge_ = {};
  std::recursive_mutex mtx_{};
  std::time_t last_upgrade_time_{0};
  map<parid_t, int32_t> epoch_replies_{};
  bool in_upgrade_epoch_{false};
  const int EPOCH_DURATION = 5;
  list<shared_ptr<RccTx>> tx_pending_execution_{};

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
    verify(0);
//    dtxn->partition_ = rhs.partition_;
//    dtxn->status_ = rhs.status_;
    verify(dtxn->id() == rhs.tid_);
    return dtxn;
  }
  shared_ptr<Tx> GetOrCreateTx(txnid_t tid, int rank, bool ro = false);

  virtual void SetPartitionId(parid_t par_id) override {
    TxLogServer::partition_id_ = par_id;
//    RccGraph::partition_id_ = par_id;
  }

  int OnDispatch(const vector<SimpleCommand> &cmd,
                 TxnOutput *output,
                 shared_ptr<RccGraph> graph);

  virtual void OnInquire(txnid_t cmd_id, int rank, map<txid_t, parent_set_t>*);

  virtual bool HandleConflicts(Tx &dtxn,
                               innid_t inn_id,
                               vector<string> &conflicts) override {
    verify(0);
  };

  void DestroyExecutor(txnid_t tid) override;

  void InquireAboutIfNeeded(RccTx &dtxn, rank_t rank);
  void InquiredGraph(RccTx &dtxn, shared_ptr<RccGraph> graph);
  bool AllAncCmt(RccTx* v, int rank);
  void WaitUntilAllPredecessorsAtLeastCommitting(RccTx* v, int rank);
  void WaitUntilAllPredSccExecuted(const RccScc &);
  void WaitNonSccParentsExecuted(const RccScc &, int rank);
  bool FullyDispatched(const RccScc &scc, rank_t r=RANK_UNDEFINED);
  bool IsExecuted(const RccScc &scc, rank_t r=RANK_UNDEFINED);
  void Decide(const RccScc &, int rank);
  bool HasICycle(const RccScc &scc);
  bool HasAbortedAncestor(const RccScc &scc);
  bool AllAncFns(const RccScc &, int rank);
  void Execute(RccScc &, int rank);
  void Execute(shared_ptr<RccTx>& );
  void Execute(RccTx&, int rank);
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
  int OnInquireValidation(txid_t tx_id, int rank);
  void OnNotifyGlobalValidation(txid_t tx_id, int rank, int validation_result);

  int OnPreAccept(txnid_t txnid,
                  rank_t rank,
                  const vector<SimpleCommand> &cmds,
                  parent_set_t& parents) ;

  int OnAccept(txnid_t txn_id,
               rank_t rank,
               const ballot_t& ballot,
               const parent_set_t& parents);

  int OnCommit(txnid_t txn_id,
               rank_t rank,
               bool need_validation,
               const parent_set_t& parents,
               TxnOutput *output);

  void __DebugExamineFridge();
  void __DebugExamineGraphVerify(RccTx &v);
  RccCommo *commo();
};

} // namespace janus
