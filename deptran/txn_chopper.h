#pragma once

#include "__dep__.h"
#include "command.h"
#include "rcc/graph.h"
#include "rcc/txn-info.h"
#include "rcc/graph.h"
#include "rcc/graph_marshaler.h"
#include "command_marshaler.h"
#include "rcc_rpc.h"
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

class TxnRequest {
 public:
  uint32_t txn_type_;
  map<int32_t, Value> input_;    // the inputs for the transactions.
  int n_try_ = 20;
  function<void(TxnReply &)> callback_ = [] (TxnReply&)->void {verify(0);};
  function<void()> fail_callback_ = [] () {verify(0);};
  void get_log(i64 tid, std::string &log);
};

enum CommandStatus {WAITING=-1, READY, ONGOING, FINISHED, INIT};

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
 public:
  map<int32_t, map<int32_t, Value> > outputs_;
  bool read_only_failed_;

  double pre_time_;

  bool early_return_ = false;
 protected:
  template<class T>
  T ChooseRandom(const std::vector<T>& v) {
    return v[rrr::RandomGenerator::rand(0,v.size()-1)];
  }
 public:
  txnid_t txn_id_; // TODO obsolete

  map<int32_t, Value> ws_ = {}; // workspace.
  map<int32_t, Value> ws_init_ = {};
  map<int32_t, map<int32_t, Value> > inputs_ = {};  // input of each piece.
  map<int32_t, int32_t> output_size_ = {};
  map<int32_t, cmdtype_t> p_types_ = {};                  // types of each piece.
  map<int32_t, parid_t> sharding_ = {};
  map<int32_t, int32_t> status_ = {}; // -1 waiting; 0 ready; 1 ongoing; 2
  // finished;
  map<int32_t, ContainerCommand*> cmds_ = {};
  std::set<parid_t> partition_ids_ = {};
  std::atomic<bool> commit_ ;

  /** server involved*/

  int n_pieces_all_ = 0;
  int n_pieces_input_ready_ = 0;
  int n_pieces_replied_ = 0;
  int n_pieces_out_ = 0;
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

//  virtual cmdtype_t type() {return txn_type_;};

  virtual int next_piece(map<int32_t, Value>* &input,
                         int &output_size,
                         int32_t &server_id,
                         int &pi,
                         int &p_type);

  virtual void Init(TxnRequest &req) = 0;

  // phase 1, res is NULL
  // phase 2, res returns SUCCESS is output is consistent with previous value

  virtual bool start_callback(int pi,
                              int res,
                              map<int32_t, Value> &output) = 0;
//  virtual bool start_callback(int pi,
//                              ChopStartResponse &res);
//  virtual bool start_callback(int pi,
//                              map<int32_t, mdb::Value> &output,
//                              bool is_defer);
  virtual bool finish_callback(ChopFinishResponse &res) { return false; }
  virtual bool IsReadOnly() = 0;
  virtual void read_only_reset();
  virtual bool read_only_start_callback(int pi,
                                        int *res,
                                        map<int32_t, Value> &output);
  virtual int GetNPieceAll() {
    return n_pieces_all_;
  }
  virtual bool IsFinished(){verify(0);}
  virtual void Merge(ContainerCommand&);
  virtual bool HasMoreSubCmdReadyNotOut();
  virtual ContainerCommand* GetNextReadySubCmd();
  virtual set<siteid_t> GetPartitionIds();

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

  virtual ~TxnCommand() { }

};

// TODO Deprecated, tries to be compatible with old naming.
typedef TxnCommand TxnChopper;


} // namespace rcc

