#include "all.h"

namespace deptran {

MicroBenchChopper::MicroBenchChopper() {
}

void MicroBenchChopper::init_W(TxnRequest &req) {
  verify(req.txn_type_ == MICRO_BENCH_W);
  txn_type_ = MICRO_BENCH_W;
  inputs_.clear();
  inputs_[0] = {
      {0, req.input_[0]},
      {1, req.input_[4]}
  };
  inputs_[1] = {
      {0, req.input_[1]},
      {1, req.input_[5]}
  };
  inputs_[2] = {
      {0, req.input_[2]},
      {1, req.input_[6]}
  };
  inputs_[3] = {
      {0, req.input_[3]},
      {1, req.input_[7]}
  };

  output_size_ = {
      {0, 0},
      {1, 0},
      {2, 0},
      {3, 0}
  };

  p_types_ = {
      {0, MICRO_BENCH_W_0},
      {1, MICRO_BENCH_W_1},
      {2, MICRO_BENCH_W_2},
      {3, MICRO_BENCH_W_3}
  };
}

void MicroBenchChopper::init(TxnRequest &req) {

  inputs_.clear();

  switch (req.txn_type_) {
    case MICRO_BENCH_R:
      init_R(req);
      break;
    case MICRO_BENCH_W:
      init_W(req);
      break;
    default:
      verify(0);
  }

  n_pieces_all_ = 4;
  callback_ = req.callback_;
  max_try_ = req.n_try_;
  n_try_ = 1;

  status_ = {
      {0, READY},
      {1, READY},
      {2, READY},
      {3, READY}
  };
  commit_.store(true);

  sss_->get_site_id_from_tb(MICRO_BENCH_TABLE_A, req.input_[0], sharding_[0]);
  sss_->get_site_id_from_tb(MICRO_BENCH_TABLE_B, req.input_[1], sharding_[1]);
  sss_->get_site_id_from_tb(MICRO_BENCH_TABLE_C, req.input_[2], sharding_[2]);
  sss_->get_site_id_from_tb(MICRO_BENCH_TABLE_D, req.input_[3], sharding_[3]);

}

void MicroBenchChopper::init_R(TxnRequest &req) {
  verify(req.txn_type_ == MICRO_BENCH_R);
  txn_type_ = MICRO_BENCH_R;
  inputs_[0] = {{0, req.input_[0]}};
  inputs_[1] = {{1, req.input_[1]}};
  inputs_[2] = {{2, req.input_[2]}};
  inputs_[3] = {{3, req.input_[3]}};

  output_size_ = {
      {0, 1},
      {1, 1},
      {2, 1},
      {3, 1}
  };

  p_types_ = {
      {0, MICRO_BENCH_R_0},
      {1, MICRO_BENCH_R_1},
      {2, MICRO_BENCH_R_2},
      {3, MICRO_BENCH_R_3}
  };

}

bool MicroBenchChopper::start_callback(const std::vector<int> &pi,
                                       int res,
                                       BatchStartArgsHelper &bsah) {
  return false;
}


bool MicroBenchChopper::start_callback(int pi,
                                       int res,
                                       map<int32_t, Value> &output) {
  return false;
}

bool MicroBenchChopper::is_read_only() {
  return false;
}

void MicroBenchChopper::retry() {
  n_pieces_out_ = 0;
  status_ = {
      {0, READY},
      {1, READY},
      {2, READY},
      {3, READY}
  };
  commit_.store(true);
  partitions_.clear();
  n_try_++;
}

MicroBenchChopper::~MicroBenchChopper() { }

} // namespace deptran
