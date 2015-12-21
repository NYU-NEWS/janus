#pragma once

#include "__dep__.h"
#include "command.h"
#include "rcc/graph.h"
#include "rcc/txn-info.h"
#include "rcc/graph.h"
#include "rcc/graph_marshaler.h"
#include "rcc_rpc.h"

namespace rococo {

class BatchStartArgsHelper;
class Coordinator;
class Sharding;

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
  std::vector<mdb::Value> input_;    // the inputs for the transactions.
  int n_try_ = 1;
  std::function<void(TxnReply &)> callback_;

  void get_log(i64 tid, std::string &log);
};

class SimpleCommand: public Command {
public:
  Command* root_;
  map<int32_t, Value> input;
  map<int32_t, Value> output;
  int output_size;
  int par_id;
  SimpleCommand() = default;
  virtual parid_t GetPar() {return par_id;}
  virtual Command* GetRootCmd() {return root_;}
};


enum CommandStatus {WAITING=-1, READY, ONGOING, FINISHED, INIT};

class TxnChopper : public Command {
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
  std::vector<map<int32_t, Value> > outputs_;
  bool read_only_failed_;

  double pre_time_;

  bool early_return_;

protected:
  txnid_t txn_id_; // TODO obsolete

 public:
  txntype_t txn_type_;

  Graph<TxnInfo> gra_;

  map<int32_t, map<int32_t, Value> > inputs_;  // input of each piece.
  //std::vector<std::vector<mdb::Value> > outputs_; // output of each piece.
  std::vector<int32_t> output_size_;
  std::vector<int32_t> p_types_;                  // types of each piece.
  std::atomic<bool> commit_;
  std::vector<uint32_t> sharding_;
  /** which server to which piece */
  std::vector<CommandStatus> status_; // -1 waiting; 0 ready; 1 ongoing; 2 finished;
  std::vector<Command*> cmd_vec_;
  std::set<parid_t> partitions_;
  /** server involved*/

  int n_pieces_all_ = 0;
  int n_pieces_out_ = 0;
  /** finished pieces counting */
  int n_finished_ = 0;

  int max_try_ = 0;
  int n_try_ = 0;

  Sharding *sss_;

  std::function<void(TxnReply &)> callback_;
  TxnReply reply_;
  struct timespec start_time_;

  TxnChopper();

  virtual cmdtype_t type() {return txn_type_;};

  /** this is even a better than better way to write hard coded coordinator.*/
  //virtual int next_piece(std::vector<std::vector<mdb::Value> > &input, int32_t &server_id, std::vector<int> &pi, std::vector<int> &p_type);

  virtual int batch_next_piece(BatchRequestHeader *batch_header,
                               std::vector<mdb::Value> &input,
                               int32_t &server_id,
                               std::vector<int> &pi,
                               Coordinator *coo);

  virtual int next_piece(map<int32_t, Value>* &input,
                         int &output_size,
                         int32_t &server_id,
                         int &pi,
                         int &p_type);

  virtual void init(TxnRequest &req) = 0;

  // phase 1, res is NULL
  // phase 2, res returns SUCCESS is output is consistent with previous value
  virtual bool start_callback(const std::vector<int> &pi,
                              int res,
                              BatchStartArgsHelper &bsah) = 0;
  virtual bool start_callback(int pi,
                              int res,
                              map<int32_t, Value> &output) = 0;
  virtual bool start_callback(int pi,
                              ChopStartResponse &res);
  virtual bool start_callback(int pi,
                              map<int32_t, mdb::Value> &output,
                              bool is_defer);
  virtual bool finish_callback(ChopFinishResponse &res) { return false; }
  virtual bool is_read_only() = 0;
  virtual void read_only_reset();
  virtual bool read_only_start_callback(int pi,
                                        int *res,
                                        map<int32_t, Value> &output);

  virtual bool IsFinished(){verify(0);}
  virtual void Merge(Command&);
  virtual bool HasMoreSubCmd();
  virtual Command* GetNextSubCmd();
  virtual set<parid_t> GetPars();

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
  virtual void retry() = 0;

  virtual ~TxnChopper() { }

};

} // namespace rcc

