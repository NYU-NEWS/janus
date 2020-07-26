#include "__dep__.h"
#include "marshal-value.h"
#include "coordinator.h"
#include "procedure.h"
#include "benchmark_control_rpc.h"

namespace janus {

static int volatile x1 =
    MarshallDeputy::RegInitializer(MarshallDeputy::CMD_VEC_PIECE,
                                   [] () -> Marshallable* {
                                     return new VecPieceData;
                                   });

TxWorkspace::TxWorkspace() {
  values_ = std::make_shared<map<int32_t, Value>>();
}

TxWorkspace::~TxWorkspace() {
//  delete values_;
}

TxWorkspace::TxWorkspace(const TxWorkspace& rhs)
    : keys_(rhs.keys_), values_{rhs.values_} {
}

void TxWorkspace::Aggregate(const TxWorkspace& rhs) {
  keys_.insert(rhs.keys_.begin(), rhs.keys_.end());
  if (values_ != rhs.values_) {
    values_->insert(rhs.values_->begin(), rhs.values_->end());
  }
}

TxWorkspace& TxWorkspace::operator=(const TxWorkspace& rhs) {
  keys_ = rhs.keys_;
  values_ = rhs.values_;
  return *this;
}

TxWorkspace& TxWorkspace::operator=(const map<int32_t, Value>& rhs) {
  keys_.clear();
  for (const auto& pair: rhs) {
    keys_.insert(pair.first);
  }
  *values_ = rhs;
  return *this;
}

Value& TxWorkspace::operator[](size_t idx) {
  keys_.insert(idx);
  return (*values_)[idx];
}

TxData::TxData() {
  clock_gettime(&start_time_);
  read_only_failed_ = false;
  pre_time_ = timespec2ms(start_time_);
  early_return_ = Config::GetConfig()->do_early_return();
}

Marshal& operator << (Marshal& m, const TxWorkspace &ws) {
  m << (ws.keys_);
  auto& input_vars = *ws.values_;
  for (int32_t k : ws.keys_) {
    auto it = input_vars.find(k);
    // allow some input vars not ready.
    if (it != input_vars.end()) {
      m << k << it->second;
    }
  }
  m << -1;
  return m;
}

Marshal& operator >> (Marshal& m, TxWorkspace &ws) {
  m >> ws.keys_;
  while (true) {
    int32_t k;
    m >> k;
    if (k >= 0) {
      Value v;
      m >> v;
      (*ws.values_)[k] = v;
    } else {
      break;
    }
  }
  return m;
}

Marshal& operator << (Marshal& m, const TxReply& reply) {
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

Marshal& operator >> (Marshal& m, TxReply& reply) {
  m >> reply.res_;
  m >> reply.output_;
  m >> reply.n_try_;
  memset(&reply.start_time_, 0, sizeof(reply.start_time_));
  m >> reply.time_;
  m >> reply.txn_type_;
  return m;
}

set<parid_t>& TxData::GetPartitionIds() {
  return partition_ids_;
}

bool TxData::IsOneRound() {
  return false;
}

vector<TxPieceData> TxData::GetCmdsByPartitionAndRank(parid_t par_id, rank_t rank) {
  vector<TxPieceData> cmds;
  for (auto& pair: map_piece_data_) {
    auto d = pair.second;
    if (d->partition_id_ == par_id && d->rank_ == rank) {
      cmds.push_back(*d);
    }
  }
  return cmds;
}

vector<TxPieceData> TxData::GetCmdsByPartition(parid_t par_id) {
  vector<TxPieceData> cmds;
  for (auto& pair: map_piece_data_) {
    auto d = pair.second;
    if (d->partition_id_ == par_id) {
      cmds.push_back(*d);
    }
  }
  return cmds;
}

ReadyPiecesData TxData::GetReadyPiecesData(int32_t max) {
  verify(n_pieces_dispatched_ < n_pieces_dispatchable_);
  verify(n_pieces_dispatched_ < n_pieces_all_);
  ReadyPiecesData ready_pieces_data;

//  int n_debug = 0;
  for (auto &kv : status_) {
    auto pi = kv.first;
    auto &status = kv.second;
    if (status == DISPATCHABLE) {
      status = INIT;
      shared_ptr<TxPieceData> piece_data = std::make_shared<TxPieceData>();
      piece_data->inn_id_ = pi;
      piece_data->partition_id_ = GetPiecePartitionId(pi);
      piece_data->type_ = pi;
      piece_data->root_id_ = id_;
      piece_data->root_type_ = type_;
      piece_data->input = inputs_[pi];
      piece_data->output_size = output_size_[pi];
      piece_data->root_ = this;
      piece_data->timestamp_ = timestamp_;
      piece_data->rank_ = ranks_[pi]; // TODO fix bug here
      map_piece_data_[pi] = piece_data;
      ready_pieces_data[piece_data->partition_id_].push_back(piece_data);
      partition_ids_.insert(piece_data->partition_id_);
      Log_debug("getting piece data piece id: %d", pi);
      verify(status_[pi] == INIT);
      status_[pi] = DISPATCHED;
      verify(type_ == type());
      verify(piece_data->root_type_ == type());
      verify(piece_data->root_type_ > 0);
      n_pieces_dispatched_++;
      max--;
      if (max == 0) break;
    }
  }
//  verify(ready_pieces_data.size() > 0);
  return ready_pieces_data;
}

shared_ptr<TxPieceData> TxData::GetNextReadySubCmd() {
  verify(0);
  verify(n_pieces_dispatched_ < n_pieces_dispatchable_);
  verify(n_pieces_dispatched_ < n_pieces_all_);

  auto sz = status_.size();
  for (auto &kv : status_) {
    auto pi = kv.first;
    auto &status = kv.second;
    if (status == DISPATCHABLE) {
      status = INIT;
      auto piece_data = std::make_shared<TxPieceData>();
      piece_data->inn_id_ = pi;
      piece_data->partition_id_ = GetPiecePartitionId(pi);
      piece_data->type_ = pi;
      piece_data->root_id_ = id_;
      piece_data->root_type_ = type_;
      piece_data->input = inputs_[pi];
      piece_data->output_size = output_size_[pi];
      piece_data->root_ = this;
      piece_data->timestamp_ = timestamp_;
      map_piece_data_[pi] = piece_data;
      partition_ids_.insert(piece_data->partition_id_);

      Log_debug("getting subcmd i: %d, thread id: %x",
                pi, std::this_thread::get_id());
      verify(status_[pi] == INIT);
      status_[pi] = DISPATCHED;

      verify(type_ == type());
      verify(piece_data->root_type_ == type());
      verify(piece_data->root_type_ > 0);
      n_pieces_dispatched_++;
      return piece_data;
    }
  }
  return nullptr;
}

bool TxData::OutputReady() {
  if (n_pieces_all_ == n_pieces_dispatch_acked_) {
    return true;
  } else {
    return false;
  }
}

void TxData::Merge(TxnOutput& output) {
  for (auto& pair: output) {
    Merge(pair.first, pair.second);
  }
}

void TxData::Merge(innid_t inn_id, map<int32_t, Value>& output) {
  verify(outputs_.find(inn_id) == outputs_.end());
  n_pieces_dispatch_acked_++;
  verify(n_pieces_all_ >= n_pieces_dispatchable_);
  verify(n_pieces_dispatchable_ >= n_pieces_dispatched_);
  verify(n_pieces_dispatched_ >= n_pieces_dispatch_acked_);
  outputs_[inn_id] = output;
  map_piece_data_[inn_id]->output = output;
  this->HandleOutput(inn_id, SUCCESS, output);
}

void TxData::Merge(CmdData &cmd) {
  auto simple_cmd = (SimpleCommand *) &cmd;
  auto pi = cmd.inn_id();
  auto &output = simple_cmd->output;
  Merge(pi, output);
}

bool TxData::HasMoreUnsentPiece() {
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

void TxData::Reset() {
  n_pieces_dispatchable_ = 0;
  n_pieces_dispatch_acked_ = 0;
  n_pieces_dispatched_ = 0;
  outputs_.clear();
}

void TxData::read_only_reset() {
  read_only_failed_ = false;
  Reset();
}
//
//bool Procedure::read_only_start_callback(int pi,
//                                          int *res,
//                                          map<int32_t, mdb::Value> &output) {
//  verify(pi < GetNPieceAll());
//  if (res == NULL) { // phase one, store outputs only
//    outputs_[pi] = output;
//  }
//  else {
//    // phase two, check if this try not failed yet
//    // and outputs is consistent with previous stored one
//    // store current outputs
//    if (read_only_failed_) {
//      *res = REJECT;
//    }
//    else if (pi >= outputs_.size()) {
//      *res = REJECT;
//      read_only_failed_ = true;
//    }
//    else if (is_consistent(outputs_[pi], output))
//      *res = SUCCESS;
//    else {
//      *res = REJECT;
//      read_only_failed_ = true;
//    }
//    outputs_[pi] = output;
//  }
//  return start_callback(pi, SUCCESS, output);
//}

double TxData::last_attempt_latency() {
  double tmp = pre_time_;
  struct timespec t_buf;
  clock_gettime(&t_buf);
  pre_time_ = timespec2ms(t_buf);
  return pre_time_ - tmp;
}

TxReply &TxData::get_reply() {
  reply_.start_time_ = start_time_;
  reply_.n_try_ = n_try_;
  struct timespec t_buf;
  clock_gettime(&t_buf);
  reply_.time_ = timespec2ms(t_buf) - timespec2ms(start_time_);
  reply_.txn_type_ = (int32_t) type_;
  return reply_;
}

void TxRequest::get_log(i64 tid, std::string &log) {
  uint32_t len = 0;
  len += sizeof(tid);
  len += sizeof(tx_type_);
  uint32_t input_size = input_.size();
  len += sizeof(input_size);
  log.resize(len);
  memcpy((void *) log.data(), (void *) &tid, sizeof(tid));
  memcpy((void *) log.data(), (void *) &tx_type_, sizeof(tx_type_));
  memcpy((void *) (log.data() + sizeof(tx_type_)), (void *) &input_size, sizeof(input_size));
  for (int i = 0; i < input_size; i++) {
    log.append(mdb::to_string(input_[i]));
  }
}

Marshal& TxData::ToMarshal(Marshal& m) const {
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

Marshal& TxData::FromMarshal(Marshal& m) {
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

} // namespace janus
