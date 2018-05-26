#include "procedure.h"

namespace janus {

MicroProcedure::MicroProcedure() {
}

void MicroProcedure::InitW(TxRequest &req) {
//  verify(req.tx_type_ == MICRO_BENCH_W);
//  type_ = MICRO_BENCH_W;
//  inputs_.clear();
//  inputs_[MICRO_BENCH_W_0] = {
//      {MICRO_VAR_K_0, req.input_[0]},
//      {MICRO_VAR_V_0, req.input_[4]}
//  };
//  inputs_[MICRO_BENCH_W_1] = {
//      {MICRO_VAR_K_1, req.input_[1]},
//      {MICRO_VAR_V_1, req.input_[5]}
//  };
//  inputs_[MICRO_BENCH_W_2] = {
//      {MICRO_VAR_K_2, req.input_[2]},
//      {MICRO_VAR_V_2, req.input_[6]}
//  };
//  inputs_[MICRO_BENCH_W_3] = {
//      {MICRO_VAR_K_3, req.input_[3]},
//      {MICRO_VAR_V_3, req.input_[7]}
//  };
//
//  output_size_ = {
//      {MICRO_BENCH_W_0, 0},
//      {MICRO_BENCH_W_1, 0},
//      {MICRO_BENCH_W_2, 0},
//      {MICRO_BENCH_W_3, 0}
//  };
//
//  p_types_ = {
//      {MICRO_BENCH_W_0, MICRO_BENCH_W_0},
//      {MICRO_BENCH_W_1, MICRO_BENCH_W_1},
//      {MICRO_BENCH_W_2, MICRO_BENCH_W_2},
//      {MICRO_BENCH_W_3, MICRO_BENCH_W_3}
//  };
}

void MicroProcedure::Init(TxRequest &req) {
  ws_init_ = req.input_;
  ws_ = req.input_;

  switch (req.tx_type_) {
    case MICRO_BENCH_R:
      InitR(req);
      break;
    case MICRO_BENCH_W:
      InitW(req);
      break;
    default:
      verify(0);
  }

  n_pieces_all_ = 4;
  callback_ = req.callback_;
  max_try_ = req.n_try_;
  n_try_ = 1;

  status_ = {
      {0, DISPATCHABLE},
      {1, DISPATCHABLE},
      {2, DISPATCHABLE},
      {3, DISPATCHABLE}
  };
  commit_.store(true);

  sss_->GetPartition(MICRO_BENCH_TABLE_A, req.input_[0], sharding_[0]);
  sss_->GetPartition(MICRO_BENCH_TABLE_B, req.input_[1], sharding_[1]);
  sss_->GetPartition(MICRO_BENCH_TABLE_C, req.input_[2], sharding_[2]);
  sss_->GetPartition(MICRO_BENCH_TABLE_D, req.input_[3], sharding_[3]);
}

void MicroProcedure::InitR(TxRequest &req) {
  verify(req.tx_type_ == MICRO_BENCH_R);
  type_ = MICRO_BENCH_R;
//  inputs_[MICRO_BENCH_R_0] = {{MICRO_VAR_K_0, req.input_[0]}};
//  inputs_[MICRO_BENCH_R_1] = {{MICRO_VAR_K_1, req.input_[1]}};
//  inputs_[MICRO_BENCH_R_2] = {{MICRO_VAR_K_2, req.input_[2]}};
//  inputs_[MICRO_BENCH_R_3] = {{MICRO_VAR_K_3, req.input_[3]}};
//
//  output_size_ = {
//      {MICRO_BENCH_R_0, 1},
//      {MICRO_BENCH_R_1, 1},
//      {MICRO_BENCH_R_2, 1},
//      {MICRO_BENCH_R_3, 1}
//  };
//
//  p_types_ = {
//      {MICRO_BENCH_R_0, MICRO_BENCH_R_0},
//      {MICRO_BENCH_R_1, MICRO_BENCH_R_1},
//      {MICRO_BENCH_R_2, MICRO_BENCH_R_2},
//      {MICRO_BENCH_R_3, MICRO_BENCH_R_3}
//  };
}

bool MicroProcedure::HandleOutput(int pi,
                                  int res,
                                  map<int32_t, Value> &output) {
  return false;
}

bool MicroProcedure::IsReadOnly() {
  return false;
}

void MicroProcedure::Reset() {
  n_pieces_dispatched_ = 0;
  n_pieces_dispatchable_ = 4;
  for (auto& pair :status_) {
    pair.second = DISPATCHABLE;
  }
//  commit_.store(true);
  partition_ids_.clear();
  n_try_++;
}

MicroProcedure::~MicroProcedure() { }

} // namespace janus
