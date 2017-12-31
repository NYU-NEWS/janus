

#include "deptran/__dep__.h"
#include "deptran/procedure.h"
#include "deptran/sharding.h"
#include "procedure.h"
#include "workload.h"

namespace janus {

void RWChopper::W_txn_init(TxRequest &req) {
//  inputs_.clear();
//  inputs_[RW_BENCHMARK_W_TXN_0] = map<int32_t, Value>({{0, req.input_[0]}});
  GetWorkspace(RW_BENCHMARK_W_TXN_0).keys_ = {0};
  n_pieces_dispatchable_ = 1;

  output_size_ = {{0,0}};
  p_types_ = {{RW_BENCHMARK_W_TXN_0, RW_BENCHMARK_W_TXN_0}};
  sss_->GetPartition(RW_BENCHMARK_TABLE, req.input_[0],
                     sharding_[RW_BENCHMARK_W_TXN_0]);
  status_ = {{RW_BENCHMARK_W_TXN_0, DISPATCHABLE}};
  n_pieces_all_ = 1;
}

void RWChopper::R_txn_init(TxRequest &req) {
//  inputs_.clear();
//  inputs_[RW_BENCHMARK_R_TXN_0] = map<int32_t, Value>({{0, req.input_[0]}});
  GetWorkspace(RW_BENCHMARK_R_TXN_0).keys_ = {0};
  n_pieces_dispatchable_ = 1;

  output_size_= {{0, 1}};
  p_types_ = {{RW_BENCHMARK_R_TXN_0, RW_BENCHMARK_R_TXN_0}};
  sss_->GetPartition(RW_BENCHMARK_TABLE, req.input_[0],
                     sharding_[RW_BENCHMARK_R_TXN_0]);
  status_ = {{RW_BENCHMARK_R_TXN_0, DISPATCHABLE}};
  n_pieces_all_ = 1;
}

RWChopper::RWChopper() {
}

void RWChopper::Init(TxRequest &req) {
  ws_init_ = req.input_;
  ws_ = req.input_;
  type_ = req.tx_type_;
  callback_ = req.callback_;
  max_try_ = req.n_try_;
  n_try_ = 1;
  commit_.store(true);
  switch (req.tx_type_) {
    case RW_BENCHMARK_W_TXN:
      W_txn_init(req);
      break;
    case RW_BENCHMARK_R_TXN:
      R_txn_init(req);
      break;
    default:
      verify(0);
  }
}

bool RWChopper::HandleOutput(int pi,
                             int res,
                             map<int32_t, Value> &output) {
  return false;
}

bool RWChopper::IsReadOnly() {
  if (type_ == RW_BENCHMARK_W_TXN)
    return false;
  else if (type_ == RW_BENCHMARK_R_TXN)
    return true;
  else
    verify(0);
}

void RWChopper::Reset() {
  TxData::Reset();
  for (auto& pair : status_) {
    pair.second = DISPATCHABLE;
  }
  commit_.store(true);
  partition_ids_.clear();
  n_pieces_dispatchable_ = 1;
  n_try_++;
}

RWChopper::~RWChopper() {
}

} // namespace janus
