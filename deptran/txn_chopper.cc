#include "__dep__.h"
#include "marshal-value.h"
#include "coordinator.h"
#include "txn_chopper.h"
#include "benchmark_control_rpc.h"

// deprecated below
//#include "rcc/rcc_srpc.h"
// deprecate above

namespace rococo {

TxnWorkspace::TxnWorkspace() {
  values_ = std::make_shared<map<int32_t, Value>>();
}

TxnWorkspace::~TxnWorkspace() {
//  delete values_;
}

TxnWorkspace::TxnWorkspace(const TxnWorkspace& rhs)
    : keys_(rhs.keys_), values_{rhs.values_} {
}

void TxnWorkspace::Aggregate(const TxnWorkspace& rhs) {
  keys_.insert(rhs.keys_.begin(), rhs.keys_.end());
  if (values_ != rhs.values_) {
    values_->insert(rhs.values_->begin(), rhs.values_->end());
  }
}

TxnWorkspace& TxnWorkspace::operator=(const TxnWorkspace& rhs) {
  keys_ = rhs.keys_;
  values_ = rhs.values_;
  return *this;
}

TxnWorkspace& TxnWorkspace::operator=(const map<int32_t, Value>& rhs) {
  keys_.clear();
  for (const auto& pair: rhs) {
    keys_.insert(pair.first);
  }
  *values_ = rhs;
  return *this;
}

Value& TxnWorkspace::operator[](size_t idx) {
  keys_.insert(idx);
  return (*values_)[idx];
}

TxnCommand::TxnCommand() {
  clock_gettime(&start_time_);
  read_only_failed_ = false;
  pre_time_ = timespec2ms(start_time_);
  early_return_ = Config::GetConfig()->do_early_return();
}

Marshal& operator << (Marshal& m, const TxnWorkspace &ws) {
//  m << (ws.keys_);
  uint64_t sz = ws.keys_.size();
  m << sz;
  for (int32_t k : ws.keys_) {
    auto it = (*ws.values_).find(k);
    verify(it != (*ws.values_).end());
    m << k << it->second;
  }
  return m;
}

Marshal& operator >> (Marshal& m, TxnWorkspace &ws) {
  uint64_t sz;
  m >> sz;
  for (uint64_t i = 0; i < sz; i++) {
    int32_t k;
    Value v;
    m >> k >> v;
    ws.keys_.insert(k);
    (*ws.values_)[k] = v;
  }
  return m;
}

Marshal& operator << (Marshal& m, const TxnReply& reply) {
  m << reply.res_;
  m << reply.output_;
  m << reply.n_try_;
  // TODO -- currently this is only used when marshalling
  // replies from forwarded requests so the source
  // (non-leader) site correctly populates this field when
  // reporting.
  // m << reply.start_time_;
  m << reply.time_;
  m << reply.txn_type_;
  return m;
}

Marshal& operator >> (Marshal& m, TxnReply& reply) {
  m >> reply.res_;
  m >> reply.output_;
  m >> reply.n_try_;
  memset(&reply.start_time_, sizeof(reply.start_time_), 0);
  m >> reply.time_;
  m >> reply.txn_type_;
  return m;
}

set<parid_t> TxnChopper::GetPartitionIds() {
  return partition_ids_;
}

bool TxnCommand::IsOneRound() {
  return false;
}

vector<SimpleCommand> TxnCommand::GetCmdsByPartition(parid_t par_id) {
  vector<SimpleCommand> cmds;
  for (auto& pair: cmds_) {
    SimpleCommand* cmd = dynamic_cast<SimpleCommand*>(pair.second);
    verify(cmd);
    if (cmd->partition_id_ == par_id) {
      cmds.push_back(*cmd);
    }
  }
  return cmds;
}

map<parid_t, vector<SimpleCommand*>> TxnCommand::GetReadyCmds(int32_t max) {
  verify(n_pieces_dispatched_ < n_pieces_dispatchable_);
  verify(n_pieces_dispatched_ < n_pieces_all_);
  map<parid_t, vector<SimpleCommand*>> cmds;

//  int n_debug = 0;
  auto sz = status_.size();
  for (auto &kv : status_) {
    auto pi = kv.first;
    auto &status = kv.second;
    if (status == DISPATCHABLE) {
      status = INIT;
      SimpleCommand *cmd = new SimpleCommand();
      cmd->inn_id_ = pi;
      cmd->partition_id_ = GetPiecePartitionId(pi);
      cmd->type_ = pi;
      cmd->root_id_ = id_;
      cmd->root_type_ = type_;
      cmd->input = inputs_[pi];
      cmd->output_size = output_size_[pi];
      cmd->root_ = this;
      cmd->timestamp_ = timestamp_;
      cmds_[pi] = cmd;
      cmds[cmd->partition_id_].push_back(cmd);
      partition_ids_.insert(cmd->partition_id_);

      Log_debug("getting subcmd i: %d, thread id: %x",
                pi, std::this_thread::get_id());
      verify(status_[pi] == INIT);
      status_[pi] = DISPATCHED;

      verify(type_ == type());
      verify(cmd->root_type_ == type());
      verify(cmd->root_type_ > 0);
      n_pieces_dispatched_++;

      max--;
      if (max == 0) break;
    }
  }

  verify(cmds.size() > 0);
  return cmds;
}

ContainerCommand *TxnCommand::GetNextReadySubCmd() {
  verify(0);
  verify(n_pieces_dispatched_ < n_pieces_dispatchable_);
  verify(n_pieces_dispatched_ < n_pieces_all_);
  SimpleCommand *cmd = nullptr;

  auto sz = status_.size();
  for (auto &kv : status_) {
    auto pi = kv.first;
    auto &status = kv.second;
    if (status == DISPATCHABLE) {
      status = INIT;
      cmd = new SimpleCommand();
      cmd->inn_id_ = pi;
      cmd->partition_id_ = GetPiecePartitionId(pi);
      cmd->type_ = pi;
      cmd->root_id_ = id_;
      cmd->root_type_ = type_;
      cmd->input = inputs_[pi];
      cmd->output_size = output_size_[pi];
      cmd->root_ = this;
      cmd->timestamp_ = timestamp_;
      cmds_[pi] = cmd;
      partition_ids_.insert(cmd->partition_id_);

      Log_debug("getting subcmd i: %d, thread id: %x",
                pi, std::this_thread::get_id());
      verify(status_[pi] == INIT);
      status_[pi] = DISPATCHED;

      verify(type_ == type());
      verify(cmd->root_type_ == type());
      verify(cmd->root_type_ > 0);
      n_pieces_dispatched_++;
      return cmd;
    }
  }

  verify(cmd);
  return cmd;
}

bool TxnCommand::OutputReady() {
  if (n_pieces_all_ == n_pieces_dispatch_acked_) {
    return true;
  } else {
    return false;
  }
}

void TxnCommand::Merge(TxnOutput& output) {
  for (auto& pair: output) {
    Merge(pair.first, pair.second);
  }
}

void TxnCommand::Merge(innid_t inn_id, map<int32_t, Value>& output) {
  verify(outputs_.find(inn_id) == outputs_.end());
  n_pieces_dispatch_acked_++;
  verify(n_pieces_all_ >= n_pieces_dispatchable_);
  verify(n_pieces_dispatchable_ >= n_pieces_dispatched_);
  verify(n_pieces_dispatched_ >= n_pieces_dispatch_acked_);
  outputs_[inn_id] = output;
  cmds_[inn_id]->output = output;
  this->start_callback(inn_id, SUCCESS, output);
}

void TxnCommand::Merge(ContainerCommand &cmd) {
  auto simple_cmd = (SimpleCommand *) &cmd;
  auto pi = cmd.inn_id();
  auto &output = simple_cmd->output;
  Merge(pi, output);
}

bool TxnCommand::HasMoreSubCmdReadyNotOut() {
  verify(n_pieces_all_ >= n_pieces_dispatchable_);
  verify(n_pieces_dispatchable_ >= n_pieces_dispatched_);
  verify(n_pieces_dispatched_ >= n_pieces_dispatch_acked_);
  if (n_pieces_dispatchable_ == n_pieces_dispatched_) {
    verify(n_pieces_all_ == n_pieces_dispatched_ ||
           n_pieces_dispatch_acked_ < n_pieces_dispatched_);
    return false;
  } else {
    verify(n_pieces_dispatchable_ > n_pieces_dispatched_);
    return true;
  }
}

void TxnCommand::Reset() {
  n_pieces_dispatchable_ = 0;
  n_pieces_dispatch_acked_ = 0;
  n_pieces_dispatched_ = 0;
  outputs_.clear();
}

void TxnCommand::read_only_reset() {
  read_only_failed_ = false;
  Reset();
}

bool TxnCommand::read_only_start_callback(int pi,
                                          int *res,
                                          map<int32_t, mdb::Value> &output) {
  verify(pi < GetNPieceAll());
  if (res == NULL) { // phase one, store outputs only
    outputs_[pi] = output;
  }
  else {
    // phase two, check if this try not failed yet
    // and outputs is consistent with previous stored one
    // store current outputs
    if (read_only_failed_) {
      *res = REJECT;
    }
    else if (pi >= outputs_.size()) {
      *res = REJECT;
      read_only_failed_ = true;
    }
    else if (is_consistent(outputs_[pi], output))
      *res = SUCCESS;
    else {
      *res = REJECT;
      read_only_failed_ = true;
    }
    outputs_[pi] = output;
  }
  return start_callback(pi, SUCCESS, output);
}

double TxnCommand::last_attempt_latency() {
  double tmp = pre_time_;
  struct timespec t_buf;
  clock_gettime(&t_buf);
  pre_time_ = timespec2ms(t_buf);
  return pre_time_ - tmp;
}

TxnReply &TxnCommand::get_reply() {
  reply_.start_time_ = start_time_;
  reply_.n_try_ = n_try_;
  struct timespec t_buf;
  clock_gettime(&t_buf);
  reply_.time_ = timespec2ms(t_buf) - timespec2ms(start_time_);
  reply_.txn_type_ = (int32_t) type_;
  return reply_;
}

void TxnRequest::get_log(i64 tid, std::string &log) {
  uint32_t len = 0;
  len += sizeof(tid);
  len += sizeof(txn_type_);
  uint32_t input_size = input_.size();
  len += sizeof(input_size);
  log.resize(len);
  memcpy((void *) log.data(), (void *) &tid, sizeof(tid));
  memcpy((void *) log.data(), (void *) &txn_type_, sizeof(txn_type_));
  memcpy((void *) (log.data() + sizeof(txn_type_)), (void *) &input_size, sizeof(input_size));
  for (int i = 0; i < input_size; i++) {
    log.append(mdb::to_string(input_[i]));
  }
}

Marshal& TxnCommand::ToMarshal(Marshal& m) const {
  m << ws_;
  m << ws_init_;
  m << inputs_;
  m << output_size_;
  m << p_types_;
  m << sharding_;
  m << status_;
  // FIXME
  //  m << cmd_;
  m << partition_ids_;
  m << n_pieces_all_;
  m << n_pieces_dispatchable_;
  m << n_pieces_dispatch_acked_;
  m << n_pieces_dispatched_;
  m << n_finished_;
  m << max_try_;
  m << n_try_;
  return m;
}

Marshal& TxnCommand::FromMarshal(Marshal& m) {
  m >> ws_;
  m >> ws_init_;
  m >> inputs_;
  m >> output_size_;
  m >> p_types_;
  m >> sharding_;
  m >> status_;
  // FIXME
  //  m >> cmd_;
  m >> partition_ids_;
  m >> n_pieces_all_;
  m >> n_pieces_dispatchable_;
  m >> n_pieces_dispatch_acked_;
  m >> n_pieces_dispatched_;
  m >> n_finished_;
  m >> max_try_;
  m >> n_try_;
  return m;
}

} // namespace rcc
