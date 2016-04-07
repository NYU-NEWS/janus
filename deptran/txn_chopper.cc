#include "__dep__.h"
#include "marshal-value.h"
#include "coordinator.h"
#include "txn_chopper.h"
#include "benchmark_control_rpc.h"
#include "batch_start_args_helper.h"

// deprecated below
//#include "rcc/rcc_srpc.h"
// deprecate above

namespace rococo {

TxnCommand::TxnCommand() {
  clock_gettime(&start_time_);
  read_only_failed_ = false;
  pre_time_ = timespec2ms(start_time_);
  early_return_ = Config::GetConfig()->do_early_return();
}


set<siteid_t> TxnChopper::GetPartitionIds() {
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

ContainerCommand *TxnCommand::GetNextReadySubCmd() {
  verify(n_pieces_out_ < n_pieces_input_ready_);
  verify(n_pieces_out_ < n_pieces_all_);
  SimpleCommand *cmd = nullptr;

  auto sz = status_.size();
  verify(sz > 0);

  for (auto &kv : status_) {
    auto pi = kv.first;
    auto &status = kv.second;

    if (status == READY) {
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
      cmds_[pi] = cmd;
      partition_ids_.insert(cmd->partition_id_);

      Log_debug("getting subcmd i: %d, thread id: %x",
                pi, std::this_thread::get_id());
      verify(status_[pi] == INIT);
      status_[pi] = ONGOING;

      //verify(cmd->type_ != 0); // not sure why this should be true???
      verify(type_ == type());
      verify(cmd->root_type_ == type());
      verify(cmd->root_type_ > 0);
      n_pieces_out_++;
      return cmd;
    }
  }

  verify(cmd);
  return cmd;
}

int TxnCommand::next_piece(
    map<int32_t, Value>* &input,
    int &output_size,
    int32_t &server_id,
    int32_t &pi,
    int32_t &p_type) {
  verify(0);
}


void TxnCommand::Merge(ContainerCommand &cmd) {
  n_pieces_replied_++;
  verify(n_pieces_all_ >= n_pieces_input_ready_);
  verify(n_pieces_input_ready_ >= n_pieces_out_);
  verify(n_pieces_out_ >= n_pieces_replied_);
  auto simple_cmd = (SimpleCommand *) &cmd;
  auto pi = cmd.inn_id();
  auto &output = simple_cmd->output;
  this->start_callback(pi, SUCCESS, output);
}

bool TxnCommand::HasMoreSubCmdReadyNotOut() {
  verify(n_pieces_all_ >= n_pieces_input_ready_);
  verify(n_pieces_input_ready_ >= n_pieces_out_);
  verify(n_pieces_out_ >= n_pieces_replied_);
  if (n_pieces_input_ready_ == n_pieces_out_) {
    verify(n_pieces_all_ == n_pieces_out_ ||
           n_pieces_replied_ < n_pieces_out_);
    return false;
  } else {
    verify(n_pieces_input_ready_ > n_pieces_out_);
    return true;
  }
}

void TxnCommand::Reset() {
  n_pieces_input_ready_ = 0;
  n_pieces_replied_ = 0;
  n_pieces_out_ = 0;
}

//bool TxnCommand::start_callback(int pi,
//                                map<int32_t, mdb::Value> &output,
//                                bool is_defer) {
//  verify(0);
//  if (is_defer && output_size_[pi] != 0)
//    early_return_ = false;
//  if (is_defer)
//    return false;
//  else
//    return start_callback(pi, SUCCESS, output);
//}
//
//bool TxnCommand::start_callback(int pi,
//                                ChopStartResponse &res) {
//  verify(0);
//  if (res.is_defered && output_size_[pi] != 0)
//    early_return_ = false;
//  if (res.is_defered)
//    return false;
//  else
//    return start_callback(pi, SUCCESS, res.output);
//}

void TxnCommand::read_only_reset() {
  read_only_failed_ = false;
  Reset();
}

bool TxnCommand::read_only_start_callback(int pi,
                                          int *res,
                                          map<int32_t, mdb::Value> &output) {
  verify(pi < GetNPieceAll());
  if (res == NULL) { // phase one, store outputs only
//    outputs_.resize(n_pieces_all_);
    outputs_[pi] = output;
  }
  else { // phase two, check if this try not failed yet
    // and outputs is consistent with previous stored one
    // store current outputs
    if (read_only_failed_) {
      *res = REJECT;
//      if (n_pieces_all_ != outputs_.size())
//        outputs_.resize(n_pieces_all_);
    }
    else if (pi >= outputs_.size()) {
      *res = REJECT;
//      outputs_.resize(n_pieces_all_);
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
  m << n_pieces_input_ready_;
  m << n_pieces_replied_;
  m << n_pieces_out_;
  m << n_finished_;
  m << max_try_;
  m << n_try_;
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
  m >> n_pieces_input_ready_;
  m >> n_pieces_replied_;
  m >> n_pieces_out_;
  m >> n_finished_;
  m >> max_try_;
  m >> n_try_;
}

} // namespace rcc
