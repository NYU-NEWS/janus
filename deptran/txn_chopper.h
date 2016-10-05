#pragma once

#include "__dep__.h"
#include "command.h"
#include "rcc/graph.h"
#include "rcc/txn-info.h"
#include "rcc/graph.h"
//#include "rcc/graph_marshaler.h"
#include "command_marshaler.h"
#include "txn_reg.h"

namespace rococo {

class BatchStartArgsHelper;
class Coordinator;
class Sharding;
class ChopStartResponse;

class TxnReply {
 public:
  int32_t res_;
  int32_t n_try_;
  struct timespec start_time_;
  double time_;
  map<int32_t, Value> output_;
  int32_t txn_type_;
};

class TxnWorkspace {
 public:
  set<int32_t> keys_ = {};
  std::shared_ptr<map<int32_t, Value>> values_{};
  TxnWorkspace();
  ~TxnWorkspace();
  TxnWorkspace(const TxnWorkspace& rhs);
  TxnWorkspace& operator= (const map<int32_t, Value> &rhs);
  TxnWorkspace& operator= (const TxnWorkspace& rhs);
  void Aggregate(const TxnWorkspace& rhs);
  Value& operator[] (size_t idx);

  size_t count(int32_t k) {
    auto r1 = keys_.count(k);
    auto r2 = (*values_).count(k);
    verify(r1 <= r2);
    return r1;
  }

  Value& at(int32_t k) {
    return (*values_).at(k);
  }

  size_t size() const {
    return keys_.size();
  }

  void insert(map<int32_t, Value>& m) {
    // TODO
    for (auto& pair : m) {
      keys_.insert(pair.first);
    }
    (*values_).insert(m.begin(), m.end());
  }
};

class TxnRequest {
 public:
  uint32_t txn_type_ = ~0;
  TxnWorkspace input_{};    // the inputs for the transactions.
  int n_try_ = 20;
  function<void(TxnReply &)> callback_ = [] (TxnReply&)->void {verify(0);};
  function<void()> fail_callback_ = [] () {verify(0);};
  void get_log(i64 tid, std::string &log);
};

Marshal& operator << (Marshal& m, const TxnWorkspace &ws);

Marshal& operator >> (Marshal& m, TxnWorkspace& ws);

Marshal& operator << (Marshal& m, const TxnReply& reply);

Marshal& operator >> (Marshal& m, TxnReply& reply);

enum CommandStatus {
  WAITING=-1,
  DISPATCHABLE=0,
  DISPATCHED=1,
  OUTPUT_READY=2,
  INIT=3
};


class SimpleCommand: public ContainerCommand {
 public:
  ContainerCommand* root_ = nullptr;
  uint64_t timestamp_{0};
  TxnWorkspace input{};
  map<int32_t, Value> output{};
  int32_t output_size = 0;
  parid_t partition_id_ = 0xFFFFFFFF;
  virtual parid_t PartitionId() const {
    verify(partition_id_ != 0xFFFFFFFF);
    return partition_id_;
  }
  virtual ContainerCommand* RootCmd() const {return root_;}
  virtual ContainerCommand* Clone() const override {
    SimpleCommand* cmd = new SimpleCommand();
    *cmd = *this;
    return cmd;
  }
  virtual ~SimpleCommand() {};
};

class BatchCommand: public ContainerCommand {
 public:
  ContainerCommand* root_ = nullptr;
  TxnWorkspace input_ = {};
  parid_t partition_id_ = ~0;
  vector<innid_t> inn_ids_ = {};
  BatchCommand() = default;
  Marshal& FromMarshal(Marshal& m) override {
    m >> input_;
    m >> partition_id_;
    m >> inn_ids_;
    return m;
  }
  Marshal& ToMarshal(Marshal& m) const override {
    m << input_;
    m << partition_id_;
    m << inn_ids_;
    return m;
  }
  virtual ~BatchCommand() {};
};

class TxnCommand: public ContainerCommand {
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
  map<innid_t, TxnWorkspace> inputs_ = {};  // input of each piece.
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
  TxnWorkspace ws_ = {}; // workspace.
  TxnWorkspace ws_init_ = {};
  TxnOutput outputs_ = {};
  map<int32_t, int32_t> output_size_ = {};
  map<int32_t, cmdtype_t> p_types_ = {};                  // types of each piece.
  map<int32_t, parid_t> sharding_ = {};
  map<int32_t, int32_t> status_ = {}; // -1 waiting; 0 ready; 1 ongoing; 2
  // finished;
  map<int32_t, SimpleCommand*> cmds_ = {};
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

  TxnRegistry *txn_reg_ = nullptr;
  Sharding *sss_ = nullptr;

  std::function<void(TxnReply &)> callback_;
  TxnReply reply_;
  struct timespec start_time_;

  TxnCommand();

  virtual void Init(TxnRequest &req) = 0;

  // phase 1, res is NULL
  // phase 2, res returns SUCCESS is output is consistent with previous value

  virtual bool start_callback(int pi,
                              int res,
                              map<int32_t, Value> &output) = 0;
  virtual bool finish_callback(ChopFinishResponse &res) { return false; }
  virtual bool IsReadOnly() = 0;
  virtual void read_only_reset();
  virtual bool read_only_start_callback(int pi,
                                        int *res,
                                        map<int32_t, Value> &output);
  virtual int GetNPieceAll() {
    return n_pieces_all_;
  }
  virtual bool OutputReady();
  virtual bool IsFinished(){verify(0);}
  virtual void Merge(ContainerCommand&);
  virtual void Merge(innid_t inn_id, map<int32_t, Value>& output);
  virtual void Merge(TxnOutput& output);
  virtual bool HasMoreSubCmdReadyNotOut();
  virtual ContainerCommand* GetNextReadySubCmd() override;
  virtual map<parid_t, vector<SimpleCommand*>> GetReadyCmds(int32_t max=0);
  virtual set<parid_t> GetPartitionIds();
  TxnWorkspace& GetWorkspace(innid_t inn_id) {
    verify(inn_id != 0);
    TxnWorkspace& ws = inputs_[inn_id];
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

  TxnReply &get_reply();

  /** for retry */
  virtual void Reset();

  virtual ~TxnCommand() {}
};

// TODO Deprecated, tries to be compatible with old naming.
typedef TxnCommand TxnChopper;


} // namespace rcc

