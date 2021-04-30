#include <memory>

#pragma once

#include "__dep__.h"
#include "command.h"
#include "rcc/graph.h"
#include "command_marshaler.h"
#include "txn_reg.h"

namespace janus {

class Coordinator;
class Sharding;
//class ChopStartResponse;

class TxReply {
 public:
  int32_t res_;
  int32_t n_try_;
  struct timespec start_time_;
  double time_;
  map<int32_t, Value> output_;
  int32_t txn_type_;
  txnid_t tx_id_;
  // int32_t n_ssid_consistent_;
  // int32_t n_decided_;
  // int32_t n_offset_valid_;
  // int32_t n_validation_passed;
  // int32_t n_cascading_aborts;
  // int32_t n_single_shard;
  // int32_t n_single_shard_write_only;
};

class TxWorkspace {
 public:
  set<int32_t> keys_ = {};
  std::shared_ptr<map<int32_t, Value>> values_{};
  std::shared_ptr<map<int32_t, shared_ptr<IntEvent>>> value_events_{};
  TxWorkspace();
  ~TxWorkspace();
  TxWorkspace(const TxWorkspace& rhs);
  TxWorkspace& operator= (const map<int32_t, Value> &rhs);
  TxWorkspace& operator= (const TxWorkspace& rhs);
  void Aggregate(const TxWorkspace& rhs);
  Value& operator[] (size_t idx);

  size_t count(int32_t k) {
    auto r1 = keys_.count(k);
    auto r2 = (*values_).count(k);
    verify(r1 <= r2);
    return r1;
  }

  Value& at(int32_t k) {
    auto it = values_->find(k);
    verify(it != values_->end());
    return it->second;
  }

  Value& WaitAt(int32_t k) {
    auto e = Reactor::CreateSpEvent<Event>();
    e->Wait([this, k](int x)->bool{
      auto it = this->values_->find(k);
      return (it != this->values_->end());
    });
    auto it = values_->find(k);
    verify(it != values_->end());
    return it->second;
  }

  size_t size() const {
    return keys_.size();
  }

  bool VerifyReady() {
    for (auto k: keys_) {
      if (values_->count(k) == 0) {
        verify(0);
        return false;
      }
    }
    return true;
  }

  void insert(map<int32_t, Value>& m) {
    // TODO
    for (auto& pair : m) {
      keys_.insert(pair.first);
    }
    (*values_).insert(m.begin(), m.end());
  }
};

class TxRequest {
 public:
  uint32_t tx_type_ = ~0;
  TxWorkspace input_{};    // the inputs for the transactions.
  int n_try_ = 20;
  int spanner_rw_reads = 0;
  int dynamic_rw_reads = 0;
  function<void(TxReply &)> callback_ = [] (TxReply&)->void {verify(0);};
  function<void()> fail_callback_ = [] () {
    verify(0);
  };
  void get_log(i64 tid, std::string &log);
};

Marshal& operator << (Marshal& m, const TxWorkspace &ws);

Marshal& operator >> (Marshal& m, TxWorkspace& ws);

Marshal& operator << (Marshal& m, const TxReply& reply);

Marshal& operator >> (Marshal& m, TxReply& reply);

enum CommandStatus {
  WAITING=-1,
  DISPATCHABLE=0,
  DISPATCHED=1,
  OUTPUT_READY=2,
  INIT=3
};

// TODO rename to TxPieceData? Seems a bad name. Should figure out a better name.
class SimpleCommand: public CmdData {
 public:
  CmdData* root_ = nullptr;
  uint64_t timestamp_{0};
  int32_t rank_{RANK_UNDEFINED};
  TxWorkspace input{};
  map<int32_t, Value> output{};
  int32_t output_size = 0;
  parid_t partition_id_ = 0xFFFFFFFF;
//  int32_t __debug_{10};
  virtual parid_t PartitionId() const {
    verify(partition_id_ != 0xFFFFFFFF);
    return partition_id_;
  }
  virtual CmdData* RootCmd() const {return root_;}
  virtual CmdData* Clone() const override {
    SimpleCommand* cmd = new SimpleCommand();
    *cmd = *this;
    return cmd;
  }
  virtual ~SimpleCommand() {};
};

typedef SimpleCommand TxPieceData;

typedef map<parid_t, vector<shared_ptr<SimpleCommand>>> ReadyPiecesData;

class VecPieceData : public Marshallable {
 public:
  // TODO move shared_ptr into the vector.
  shared_ptr<vector<shared_ptr<SimpleCommand>>> sp_vec_piece_data_{};
  VecPieceData() : Marshallable(MarshallDeputy::CMD_VEC_PIECE) {

  }

  Marshal& ToMarshal(Marshal& m) const override {
    verify(sp_vec_piece_data_);
    m << (int32_t) sp_vec_piece_data_->size();
    for (auto sp : *sp_vec_piece_data_) {
      m << *sp;
    }
//    m << *sp_vec_piece_data_;
    return m;
  }

  Marshal& FromMarshal(Marshal& m) override {
    verify(!sp_vec_piece_data_);
    sp_vec_piece_data_ = std::make_shared<vector<shared_ptr<TxPieceData>>>();
    int32_t sz;
    m >> sz;
    for (int i = 0; i < sz; i++) {
      auto x = std::make_shared<TxPieceData>();
      m >> *x;
      sp_vec_piece_data_->push_back(x);
    }
//    m >> *sp_vec_piece_data_;
    return m;
  }
};

/**
 * input ready levels:
 *   1. shard ready
 *   2. conflict ready
 *   3. all (execute) ready
 */
class TxData: public CmdData {
 private:
  static inline bool is_consistent(map<int32_t, Value> &previous,
                                   map<int32_t, Value> &current) {
    if (current.size() != previous.size())
      return false;
    for (size_t i = 0; i < current.size(); i++)
      if (current[i] != previous[i])
        return false;
    return true;
  }
  map<innid_t, TxWorkspace> inputs_ = {};  // input of each piece.
 public:
  bool read_only_failed_ = false;
  double pre_time_ = 0.0;
  bool early_return_ = false;
 protected:
  template<class T>
  T ChooseRandom(const std::vector<T>& v) {
    return v[rrr::RandomGenerator::rand(0,v.size()-1)];
  }
 public:
  txnid_t txn_id_; // TODO obsolete
  uint64_t timestamp_ = 0;
  TxWorkspace ws_ = {}; // workspace.
  TxWorkspace ws_init_ = {};
  TxnOutput outputs_ = {};
  map<int32_t, int32_t> output_size_ = {};
  map<int32_t, cmdtype_t> p_types_ = {};                  // types of each piece.
  map<int32_t, parid_t> sharding_ = {};
  map<int32_t, int32_t> status_ = {}; // -1 waiting; 0 ready; 1 ongoing; 2
  map<innid_t, rank_t> ranks_ = {};  // input of each piece.
  // finished;
  map<int32_t, shared_ptr<TxPieceData>> map_piece_data_ = {};
  std::set<parid_t> partition_ids_ = {};
  std::atomic<bool> commit_ ;

  /** server involved */
  int n_pieces_all_ = 0;
  int n_pieces_dispatchable_ = 0;
  int n_pieces_dispatch_acked_ = 0;
  int n_pieces_dispatched_ = 0;
  /** finished pieces counting */
  int n_finished_ = 0;

  int max_try_ = 0;
  int n_try_ = 0;
  // ACC stats related
  // int n_ssid_consistent_ = 0;
  // int n_decided_ = 0;
  // int n_offset_valid_ = 0;
  // int n_validation_passed = 0;
  // int n_cascading_aborts = 0;
  // int n_single_shard = 0;
  // int n_single_shard_write_only = 0;

  bool validation_ok_{true};
  bool need_validation_{false};

  weak_ptr<TxnRegistry> txn_reg_{};
  Sharding *sss_ = nullptr;

  std::function<void(TxReply &)> callback_;
  TxReply reply_;
  struct timespec start_time_;

  TxData();

  virtual void Init(TxRequest &req) = 0 ;

  // phase 1, res is NULL
  // phase 2, res returns SUCCESS is output is consistent with previous value
  virtual bool HandleOutput(int pi,
                            int res,
                            map<int32_t, Value> &output) = 0;
  virtual bool IsReadOnly() = 0;
  virtual void read_only_reset();
  virtual int GetNPieceAll() {
    return n_pieces_all_;
  }
  virtual bool OutputReady();
  virtual bool IsFinished(){verify(0);}
  virtual void Merge(CmdData&) override;
  virtual void Merge(innid_t inn_id, map<int32_t, Value>& output);
  virtual void Merge(TxnOutput& output);
  virtual bool HasMoreUnsentPiece();
  virtual bool AllPiecesDispatchable();
  virtual shared_ptr<TxPieceData> GetNextReadySubCmd();
  virtual ReadyPiecesData GetReadyPiecesData(int32_t max = 0);
  virtual set<parid_t>& GetPartitionIds() override;
  TxWorkspace& GetWorkspace(innid_t inn_id) {
    verify(inn_id != 0);
    TxWorkspace& ws = inputs_[inn_id];
    if (ws.values_->size() == 0)
      ws.values_ = ws_.values_;
    return ws;
  }

  virtual parid_t GetPiecePartitionId(innid_t inn_id) {
    verify(sharding_.find(inn_id) != sharding_.end());
    return sharding_[inn_id];
  }
  virtual bool IsOneRound();
  vector<SimpleCommand> GetCmdsByPartition(parid_t par_id);
  vector<SimpleCommand> GetCmdsByPartitionAndRank(parid_t par_id, rank_t rank);

  Marshal& ToMarshal(Marshal& m) const override;
  Marshal& FromMarshal(Marshal& m) override;

  inline bool can_retry() {
    return (max_try_ == 0 || n_try_ < max_try_);
  }

  inline bool do_early_return() {
    return early_return_;
  }

  inline void disable_early_return() {
    early_return_ = false;
  }

  double last_attempt_latency();

  TxReply &get_reply();

  /** for retry */
  virtual void Reset() override;

  virtual ~TxData() {}

    /* ACC members */
    bool _is_consistent = true;
    //bool _validate_abort = false;
    // bool _offset_invalid = false;
    // a consistent snapshot resides between highest_low and lowest_high
    snapshotid_t highest_ssid_low = 0;
    snapshotid_t lowest_ssid_high = UINT64_MAX;
    snapshotid_t ssid_new = 0;
    //snapshotid_t highest_ssid_high = 0;  // for updating ssids upon validation
    // and for final ssid if validation says consistent
    int n_validate_rpc_ = 0;
    int n_validate_ack_ = 0;
    snapshotid_t ssid_spec = 0;  // the speculative ssid provided to servers, generated by ML engine
    bool _decided = true;        // the reads in this txn returning finalized write, no need to wait
    bool _status_query_done = false;
    bool _status_abort = false;
    int n_status_query = 0;
    int n_status_callback = 0;
    int n_abort_sent = 0;
    int n_abort_ack = 0;
    bool _validation_failed = false;
    bool _early_abort = false;

    // failure handling
    std::unordered_set<parid_t> cohorts = {};
    parid_t coord = UINT32_MAX;

    // for rotxn
    bool is_rotxn = false;
    std::unordered_set<parid_t> par_ids = {};
    bool is_rotxn_okay = true;

    // ssid predictor related below
    std::unordered_map<innid_t, parid_t> innid_to_server;
    std::unordered_map<innid_t, uint64_t> innid_to_starttime;
    std::set<parid_t> pars_to_finalize;  // the accessed partitions that this txn writes to
};

} // namespace rcc

