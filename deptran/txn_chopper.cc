#include "__dep__.h"
#include "marshal-value.h"
#include "coordinator.h"
#include "txn_chopper.h"
#include "benchmark_control_rpc.h"
#include "batch_start_args_helper.h"
//#include "all.h"

namespace rococo {

TxnChopper::TxnChopper() {
  clock_gettime(&start_time_);
  read_only_failed_ = false;
  pre_time_ = timespec2ms(start_time_);
  early_return_ = Config::GetConfig()->do_early_return();
}

Command *TxnChopper::GetNextSubCmd(map<innid_t, Command *> &cmdmap) {
  if (n_started_ == n_pieces_) {
    return nullptr;
  }
  SimpleCommand *cmd = nullptr;

  for (int i = 0; i < status_.size(); i++) {
    if (status_[i] == 0) {
      cmd = new SimpleCommand();
      cmd->inn_id_ = i;
      cmd->par_id = sharding_[i];
      cmd->type_ = p_types_[i];
      cmd->input = inputs_[i];
      cmd->output_size = output_size_[i];
      cmd->root_ = this;
      proxies_.insert(sharding_[i]);
      Log_debug("getting subcmd i: %d, thread id: %x",
                i, std::this_thread::get_id());
      verify(status_[i] == 0);
      status_[i] = 1;
      verify(cmd->type_ != 0);
      return cmd;
    }
  }
  return cmd;
}


int TxnChopper::next_piece(
    std::vector<mdb::Value> *&input,
    int &output_size,
    int32_t &server_id,
    int32_t &pi,
    int32_t &p_type) {

  if (n_started_ == n_pieces_) {
    return 2;
  }

  int status = 1;
  for (int i = 0; i < status_.size(); i++) {
    if (status_[i] == 0) {
      server_id = sharding_[i];
      proxies_.insert(server_id);
      pi = i;
      p_type = p_types_[i];
      input = &inputs_[i];
      status_[i] = 1;
      output_size = output_size_[i];
      return 0;
    } else if (status_[i] == 1) {
    } else if (status_[i] == -1) {
      status = -1;
    }
  }

  if (status == 1) {
    return 1;   // all pieces are ongoing.
  } else if (status == -1) {
    return -1;  // some pieces are not ready.
  } else {
    verify(0);
  }
}

int TxnChopper::batch_next_piece(BatchRequestHeader *batch_header,
                                 std::vector<mdb::Value> &input,
                                 int32_t &server_id, std::vector<int> &pi,
                                 Coordinator *coo) {
  verify(0);
//  if (n_started_ == n_pieces_)
//    return 2;
//
//  int status = 1;
//
//  pi.reserve(status_.size());
//  pi.clear();
//  server_id = 0;
//  uint32_t input_size = 0;
//  for (int i = 0; i < status_.size(); i++) {
//    if (status_[i] == 0) {
//      if (pi.size() == 0) {
//        server_id = sharding_[i];
//        proxies_.insert(server_id);
//        pi.push_back(i);
//        input_size += inputs_[i].size();
//        status_[i] = 1;
//      }
//      else if (server_id == sharding_[i]) {
//        pi.push_back(i);
//        input_size += inputs_[i].size();
//        status_[i] = 1;
//      }
//    }
//    else if (status_[i] == -1)
//      status = -1;
//  }
//
//  if (pi.size() > 0) {
//    BatchStartArgsHelper bsah;
//    bsah.init_input_client(&input, pi.size(), input_size);
//    batch_header->expected_output_size = 0;
//    std::vector<int>::iterator it = pi.begin();
//    for (; it != pi.end(); it++) {
//      bsah.put_next_input_client(inputs_[*it], (i32) p_types_[*it], (i64) coo->next_pie_id(), (i32) output_size_[*it]);
//      batch_header->expected_output_size += (i32) output_size_[*it];
//    }
//
//    batch_header->num_piece = (i32) pi.size();
//
//    return 0;
//  }
//
//  return status;
}

void TxnChopper::Merge(Command &cmd) {
  auto simple_cmd = (SimpleCommand *) &cmd;
  auto pi = cmd.inn_id();
  auto &output = simple_cmd->output;
  this->start_callback(pi, SUCCESS, output);
}

bool TxnChopper::HasMoreSubCmd(map<innid_t, Command *> &cmdmap) {
  verify(cmdmap.size() <= n_pieces_);
//  if (cmdmap.size() == n_pieces_)
  if (n_pieces_ == n_started_)
    return false;
  else
    return true;
}

bool TxnChopper::start_callback(int pi,
                                std::vector<mdb::Value> &output,
                                bool is_defer) {
  verify(0);
  if (is_defer && output_size_[pi] != 0)
    early_return_ = false;
  if (is_defer)
    return false;
  else
    return start_callback(pi, SUCCESS, output);
}

bool TxnChopper::start_callback(int pi,
                                const ChopStartResponse &res) {
  verify(0);
  if (res.is_defered && output_size_[pi] != 0)
    early_return_ = false;
  if (res.is_defered)
    return false;
  else
    return start_callback(pi, SUCCESS, res.output);
}

void TxnChopper::read_only_reset() {
  read_only_failed_ = false;
  retry();
}

bool TxnChopper::read_only_start_callback(int pi, int *res, const std::vector<mdb::Value> &output) {
  verify(pi < n_pieces_);
  if (res == NULL) { // phase one, store outputs only
    outputs_.resize(n_pieces_);
    outputs_[pi] = output;
  }
  else { // phase two, check if this try not failed yet
    // and outputs is consistent with previous stored one
    // store current outputs
    if (read_only_failed_) {
      *res = REJECT;
      if (n_pieces_ != outputs_.size())
        outputs_.resize(n_pieces_);
    }
    else if (pi >= outputs_.size()) {
      *res = REJECT;
      outputs_.resize(n_pieces_);
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

//int TxnChopper::next_piece(
//        std::vector<std::vector<mdb::Value> > &input,
//        int32_t& server_id,
//        std::vector<int32_t> &pi,
//        std::vector<int32_t> &p_type) {
//
//    input.clear();
//    pi.clear();
//    p_type.clear();
//    if (n_started_ == n_pieces_) {
//        return 2;
//    }
//
//    bool server_set = false;
//    int status = 1;
//    for (int i = 0; i < status_.size(); i++) {
//        if (status_[i] == 0) {
//            if (!server_set) {
//                server_set = true;
//                server_id = sharding_[i];
//                proxies_.insert(server_id);
//                pi.push_back(i);
//                p_type.push_back(p_types_[i]);
//                input.push_back(inputs_[i]);
//                status_[i] = 1;
//            }
//            else if (server_id == sharding_[i]) {
//                pi.push_back(i);
//                p_type.push_back(p_types_[i]);
//                input.push_back(inputs_[i]);
//                status_[i] = 1;
//            }
//        } else if (status_[i] == 1) {
//        } else if (status_[i] == -1) {
//            status = -1;
//        }
//    }
//
//    if (server_set)
//        return 0;
//
//    if (status == 1) {
//        return 1;   // all pieces are ongoing.
//    } else if (status == -1) {
//        return -1;  // some pieces are not ready.
//    } else {
//        verify(0);
//    }
//}

double TxnChopper::last_attempt_latency() {
  double tmp = pre_time_;
  struct timespec t_buf;
  clock_gettime(&t_buf);
  pre_time_ = timespec2ms(t_buf);
  return pre_time_ - tmp;
}

TxnReply &TxnChopper::get_reply() {
  reply_.start_time_ = start_time_;
  reply_.n_try_ = n_try_;
  struct timespec t_buf;
  clock_gettime(&t_buf);
  reply_.time_ = timespec2ms(t_buf) - timespec2ms(start_time_);
  reply_.txn_type_ = (int32_t) txn_type_;
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

} // namespace rcc
