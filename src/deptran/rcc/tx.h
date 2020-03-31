#pragma once
#include "../__dep__.h"
#include "graph.h"
#include "../tx.h"
#include "../scheduler.h"
#include "../procedure.h"

#define PHASE_RCC_DISPATCH (1)
#define PHASE_RCC_COMMIT (2)
#define PHASE_RCC_D_DISPATCH (3)
#define PHASE_RCC_D_COMMIT (3)

namespace janus {

class RccTx: public Tx, public Vertex<RccTx> {
 public:
  void __DebugCheckParents();
  void __DebugCheckScc();
  static SpinLock __debug_parents_lock_;
  static SpinLock __debug_scc_lock_;
  static std::map<txid_t, parent_set_t> __debug_parents_;
  static std::map<txid_t, vector<txid_t>> __debug_scc_;
  static thread_local uint64_t timestamp_;
  bool __mocking_janus_{false};
  rank_t current_rank_{RANK_UNDEFINED};
  rank_t shared_rank_{RANK_UNDEFINED}; // this could be greater than current_rank_ as it can be updated during graph propagation
  rank_t scc_rank_{0};
  // ----below variables get reset whenever current_rank_ is changed
  SharedIntEvent status_{};
  ballot_t max_seen_ballot_{0};
  ballot_t max_accepted_ballot_{0};
  SharedIntEvent commit_received_{};
  SharedIntEvent log_apply_finished_{};
  shared_ptr<IntEvent> sp_ev_commit_{Reactor::CreateSpEvent<IntEvent>()};
  TxnOutput *p_output_reply_ = nullptr;
  TxnOutput output_ = {};
  bool log_apply_started_{false}; // compared to ???
  // also reset phase_ and fully_dispatched.
  // ----above variables get reset whenever current_rank_ is changed
  bool need_validation_{false};
  bool __commit_received_{false};

  bool all_ancestors_committing() {
    return all_anc_cmt_hint;
  }

  class StatusBox : public BoxEvent<int> {
   public:
    int& Get() {
      verify(is_set_);
      return BoxEvent<int>::Get();
    }
    void Set(const int& x) {
//      verify(x != REJECT);
      BoxEvent<int>::Set(x);
    }
  };

  bool __debug_local_validated_foreign_{false};
  shared_ptr<StatusBox> local_validated_{Reactor::CreateSpEvent<StatusBox>()};
  shared_ptr<StatusBox> global_validated_{Reactor::CreateSpEvent<StatusBox>()};

  vector<SimpleCommand> dreqs_ = {};
  bool read_only_ = false;
  bool __debug_replied = false;
  vector<void**> external_refs_{};
  map<Row *, map<colid_t, mdb::version_t>> read_vers_{};

  // hopefully this makes involve checks faster
  enum InvolveEnum {UNKNOWN, INVOLVED, FOREIGN};
  InvolveEnum involve_flag_{UNKNOWN};

//  bool InvolveThisShard() {
//    if (involve_flag_ == UNKNOWN) {
//      verify(status() >= TXN_CMT);
//      if (Involve(TxLogServer::partition_id_)) {
//        involve_flag_ = INVOLVED;
//      } else {
//        involve_flag_ = FOREIGN;
//      }
//    }
//    return (involve_flag_ == INVOLVED);
//  }

  // if any other transactions is blocked by this transaction,
  // add it to this set to make the checking waitlist faster.
  set<RccTx*> to_checks_{};

  RccTx() = delete;
  RccTx(txnid_t id);
  RccTx(RccTx& rhs_dtxn);
  RccTx(epoch_t, txnid_t tid, TxLogServer *mgr, bool ro);

  virtual ~RccTx() {
    for (auto& ref : external_refs_) {
      if (*ref == this) {
        *ref = nullptr;
      }
    }
  }

  void CommitRank() {
    verify(0);
    if (current_rank_ == RANK_D) {
      current_rank_ = RANK_MAX;
    } else if (current_rank_ == RANK_I) {
      current_rank_ = RANK_D;
      status_.value_ = TXN_UNKNOWN;
    } else {
      verify(0);
    }
    if (current_rank_ > shared_rank_) {
      shared_rank_ = current_rank_;
    }

    max_seen_ballot_ = 0;
    max_accepted_ballot_ = 0;
    sp_ev_commit_ = Reactor::CreateSpEvent<IntEvent>();
    p_output_reply_ = nullptr;
    // TODO do we need to proper reset this value.
//    status_.value_ = TXN_UNKNOWN;
    fully_dispatched_ = Reactor::CreateSpEvent<IntEvent>();
//    fully_dispatched_ = false;
    log_apply_started_ = false;
  }

  virtual void DispatchExecute(SimpleCommand &cmd,
                               map<int32_t, Value> *output);

  void CommitValidate();
  virtual void CommitExecute();

  virtual void Abort();

  bool ReadColumn(mdb::Row *row,
                  mdb::colid_t col_id,
                  Value *value,
                  int hint_flag) override;

  bool WriteColumn(Row *row,
                   colid_t col_id,
                   const Value &value,
                   int hint_flag = TXN_INSTANT) override;

  void TraceDep(Row* row, colid_t col_id, rank_t hint_flag);
  void CommitDep(Row& row, colid_t col_id);

  virtual void AddParentEdge(shared_ptr<RccTx> other, int8_t weight) override;

  virtual void start_ro(const SimpleCommand&,
                        map<int32_t, Value> &output,
                        rrr::DeferredReply *defer);


  virtual mdb::Row *CreateRow(
      const mdb::Schema *schema,
      const std::vector<mdb::Value> &values) override {
    return RccRow::create(schema, values);
  }

  virtual bool UpdateStatus(int s) {
    if (status_.value_ < s) {
      status_.Set(status_.value_ | s);
      if (status_.value_ >= TXN_CMT) {
        __DebugCheckParents();
      }
      return true;
    } else {
      return false;
    }
  }

 public:
  std::set<uint32_t> partition_;
  std::vector<uint64_t> pieces_;
  bool during_commit = false;
  bool during_asking = false;
  bool inquire_acked_ = false;
  bool all_anc_cmt_hint{false};

  txnid_t id() override {
    return tid_;
  }

  inline int8_t status() const {
    return status_.value_;
  }

  inline bool IsAborted() const {
    return (status_.value_ & TXN_ABT);
  }

  inline bool IsCommitting() const {
    return (status_.value_ & TXN_CMT);
  }

  inline bool IsDecided() const {
    return (status_.value_ & TXN_DCD);
  }

  inline bool HasLogApplyStarted() const {
//    if (executed_) {
//      verify(IsDecided());
//    }
    return log_apply_started_;
  }

  bool Involve(parid_t id) {
    if (involve_flag_ == INVOLVED)
      return true;
    if (involve_flag_ == FOREIGN)
      return false;
//    verify(partition_.size() > 0);
    auto it = partition_.find(id);
    if (it == partition_.end()) {
      if (IsCommitting()) {
        involve_flag_ = FOREIGN;
      }
      return false;
    } else {
      if (IsCommitting()) {
        involve_flag_ = INVOLVED;
      }
      return true;
    }
  }

  uint32_t random_server() {
    verify(partition_.size() > 0);
    uint32_t i = RandomGenerator::rand(0, partition_.size() - 1);
    auto it = partition_.begin();
    std::advance(it, i);
    uint32_t id = *(partition_.begin());
    Log::debug("random a related server, id: %x", id);
    return id;
  }

  bool operator<(const RccTx &rhs) const {
    return tid_ < rhs.tid_;
  }

  inline void set_id(uint64_t id) {
    tid_ = id;
  }

  inline void union_data(const RccTx &ti,
                         bool trigger = false,
                         bool server = false) {
    partition_.insert(ti.partition_.begin(), ti.partition_.end());
    union_status(ti.status_.value_, trigger, server);
  }

  inline void union_status(int8_t status,
                    bool is_trigger = false,
                    bool is_server = false) {

    if (false) {
      // TODO
      // I cannot change the status of this txn.
    } else {
#ifdef DEBUG_CODE
      verify((status_ | status) >= status_);
#endif
      status_.Set(status_.value_ |= status);

    }
  }

};

typedef map<txid_t, ParentEdge<RccTx>> parent_set_t;

inline rrr::Marshal &operator<<(rrr::Marshal &m, const ParentEdge<RccTx> &e) {
  m << e.partitions_;
  return m;
}

inline rrr::Marshal &operator>>(rrr::Marshal &m, ParentEdge<RccTx> &e) {
  m >> e.partitions_;
  return m;
}

inline rrr::Marshal &operator<<(rrr::Marshal &m, const RccTx &ti) {
  m << ti.tid_ << ti.status() << ti.partition_ << ti.parents_;
  return m;
}

inline rrr::Marshal &operator>>(rrr::Marshal &m, RccTx &ti) {
  int8_t status;
  m >> ti.tid_ >> status >> ti.partition_ >> ti.parents_;
  ti.union_status(status);
  return m;
}

} // namespace janus
