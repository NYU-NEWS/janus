#pragma once
#include "../__dep__.h"
#include "deptran/tx.h"
#include "deptran/procedure.h"

#define PHASE_RCC_DISPATCH (1)
#define PHASE_RCC_COMMIT (2)

namespace janus {
class TxRococo: public Tx, public Vertex<TxRococo> {
 public:
//  int8_t status_ = TXN_UKN;
  SharedIntEvent status_;
  ballot_t max_seen_ballot_{0};
  ballot_t max_accepted_ballot_{0};

  vector<SimpleCommand> dreqs_ = {};
  TxnOutput *ptr_output_repy_ = nullptr;
  TxnOutput output_ = {};
  vector<TxnInfo *> conflict_txns_ = {}; // This is read-only transaction
  function<void(int)> finish_reply_callback_ =  [] (int) -> void {verify(0);};
  bool commit_request_received_ = false;
  bool read_only_ = false;
  bool __debug_replied = false;
  vector<void**> external_refs_{};

  shared_ptr<IntEvent> sp_ev_commit_{Reactor::CreateSpEvent<IntEvent>()};

  // hopefully this makes involve checks faster
  enum InvolveEnum {UNKNOWN, INVOLVED, FOREIGN};
  InvolveEnum involve_flag_{UNKNOWN};

  // if any other transactions is blocked by this transaction,
  // add it to this set to make the checking waitlist faster.
  set<TxRococo*> to_checks_{};

  TxRococo() = delete;
  TxRococo(txnid_t id);
  TxRococo(TxRococo& rhs_dtxn);
  TxRococo(epoch_t, txnid_t tid, Scheduler *mgr, bool ro);

  virtual ~TxRococo() {
    for (auto& ref : external_refs_) {
      if (*ref == this) {
        *ref = nullptr;
      }
    }
  }

  virtual void DispatchExecute(SimpleCommand &cmd,
                               map<int32_t, Value> *output);

  virtual void CommitExecute();

  virtual void Abort();

  virtual void ReplyFinishOk();

  bool ReadColumn(mdb::Row *row,
                  mdb::colid_t col_id,
                  Value *value,
                  int hint_flag) override;

  bool WriteColumn(Row *row,
                   colid_t col_id,
                   const Value &value,
                   int hint_flag = TXN_INSTANT) override;

  void TraceDep(Row* row, colid_t col_id, int hint_flag);

  virtual void AddParentEdge(shared_ptr<TxRococo> other, int8_t weight) override;

  virtual bool start_exe_itfr(
      defer_t defer,
      ProcHandler &handler,
      const SimpleCommand& cmd,
      map<int32_t, Value> *output
  );

  virtual void start_ro(const SimpleCommand&,
                        map<int32_t, Value> &output,
                        rrr::DeferredReply *defer);


  virtual mdb::Row *CreateRow(
      const mdb::Schema *schema,
      const std::vector<mdb::Value> &values) override {
    return RCCRow::create(schema, values);
  }

  virtual void kiss(mdb::Row *r,
                    int col,
                    bool immediate);

  virtual bool UpdateStatus(int s) {
    if (status_.value_ < s) {
      status_.Set(s);
      return true;
    } else {
      return false;
    }
  }

  // below is copied from txn-info
// private:
//  int8_t status_{TXN_UKN};

 public:
  std::set<uint32_t> partition_;
  std::vector<uint64_t> pieces_;
  bool fully_dispatched{false};
  bool executed_{false};
  bool during_commit = false;
  bool during_asking = false;
  bool inquire_acked_ = false;
  bool all_anc_cmt_hint{false};
//  RccGraph* graph_{nullptr};

  vector<shared_ptr<IntEvent>> vec_sp_inquire_{};

  ChopFinishResponse *res = nullptr;
//
//  RccDTxn(uint64_t id) : DTxn(id, nullptr) {
//    txn_id_ = id;
//  }
//
//  ~RccDTxn() {
//    Log_debug("txn info destruct, id: %llx", txn_id_);
//    txn_id_ = 0; // for debug purpose
//  }

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

  inline bool IsExecuted() const {
//    if (executed_) {
//      verify(IsDecided());
//    }
    return executed_;
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

  bool operator<(const TxRococo &rhs) const {
    return tid_ < rhs.tid_;
  }

  inline void set_id(uint64_t id) {
    tid_ = id;
  }

  inline void union_data(const TxRococo &ti,
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


inline rrr::Marshal &operator<<(rrr::Marshal &m, const TxRococo &ti) {
  m << ti.tid_ << ti.status() << ti.partition_ << ti.epoch_;
  return m;
}

inline rrr::Marshal &operator>>(rrr::Marshal &m, TxRococo &ti) {
  int8_t status;
  m >> ti.tid_ >> status >> ti.partition_ >> ti.epoch_;
  ti.union_status(status);
  return m;
}

} // namespace janus
