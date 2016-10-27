#include "config.h"
#include "constants.h"
#include "sharding.h"
#include "txn_req_factory.h"

// for tpca benchmark
#include "bench/tpca/piece.h"
#include "bench/tpca/chopper.h"

// tpcc benchmark
#include "bench/tpcc/piece.h"
#include "bench/tpcc/chopper.h"

// tpcc dist partition benchmark
#include "bench/tpcc_dist/piece.h"
#include "bench/tpcc_dist/chopper.h"

// tpcc real dist partition benchmark
#include "bench/tpcc_real_dist/piece.h"
#include "bench/tpcc_real_dist/chopper.h"

// rw benchmark
#include "bench/rw_benchmark/piece.h"
#include "bench/rw_benchmark/chopper.h"

// micro bench
#include "bench/micro/piece.h"
#include "bench/micro/chopper.h"

namespace rococo {

TxnGenerator::TxnGenerator(Config* config)
    : txn_weight_(config->get_txn_weight()),
      txn_weights_(config->get_txn_weights()),
      sharding_(config->sharding_) {
  benchmark_ = Config::GetConfig()->get_benchmark();
  n_try_ = Config::GetConfig()->get_max_retry();
  single_server_ = Config::GetConfig()->get_single_server();

  std::map<std::string, uint64_t> table_num_rows;
  sharding_->get_number_rows(table_num_rows);

  if (Config::GetConfig()->dist_ == "fixed") {
    single_server_ = Config::SS_PROCESS_SINGLE;
  }

  switch (benchmark_) {
    case MICRO_BENCH:
      micro_bench_para_.n_table_a_ = table_num_rows[std::string(MICRO_BENCH_TABLE_A)];
      micro_bench_para_.n_table_b_ = table_num_rows[std::string(MICRO_BENCH_TABLE_B)];
      micro_bench_para_.n_table_c_ = table_num_rows[std::string(MICRO_BENCH_TABLE_C)];
      micro_bench_para_.n_table_d_ = table_num_rows[std::string(MICRO_BENCH_TABLE_D)];
      break;
    case TPCA:
    case TPCC:
    case TPCC_DIST_PART:
    case TPCC_REAL_DIST_PART: {
      break;
    }
    case RW_BENCHMARK:
      rw_benchmark_para_.n_table_ = table_num_rows[std::string(RW_BENCHMARK_TABLE)];
      fix_id_ = (Config::GetConfig()->dist_ == "fixed") ?
                RandomGenerator::rand(0, rw_benchmark_para_.n_table_) :
                -1;
      break;
    default:
      Log_fatal("benchmark not implemented");
      verify(0);
  }
}


void TxnGenerator::get_micro_bench_read_req(TxnRequest *req, uint32_t cid) const {
  req->txn_type_ = MICRO_BENCH_R;
  req->input_[0] = Value((i32) RandomGenerator::rand(0, micro_bench_para_.n_table_a_ - 1));
  req->input_[1] = Value((i32) RandomGenerator::rand(0, micro_bench_para_.n_table_b_ - 1));
  req->input_[2] = Value((i32) RandomGenerator::rand(0, micro_bench_para_.n_table_c_ - 1));
  req->input_[3] = Value((i32) RandomGenerator::rand(0, micro_bench_para_.n_table_d_ - 1));
}

void TxnGenerator::get_micro_bench_write_req(TxnRequest *req, uint32_t cid) const {
  req->txn_type_ = MICRO_BENCH_W;
  req->input_[0] = Value((i32) RandomGenerator::rand(0, micro_bench_para_.n_table_a_ - 1));
  req->input_[1] = Value((i32) RandomGenerator::rand(0, micro_bench_para_.n_table_b_ - 1));
  req->input_[2] = Value((i32) RandomGenerator::rand(0, micro_bench_para_.n_table_c_ - 1));
  req->input_[3] = Value((i32) RandomGenerator::rand(0, micro_bench_para_.n_table_d_ - 1));
  req->input_[4] = Value((i64) RandomGenerator::rand(0, 1000));
  req->input_[5] = Value((i64) RandomGenerator::rand(0, 1000));
  req->input_[6] = Value((i64) RandomGenerator::rand(0, 1000));
  req->input_[7] = Value((i64) RandomGenerator::rand(0, 1000));
}

void TxnGenerator::get_micro_bench_txn_req(
    TxnRequest *req, uint32_t cid) const {
  req->n_try_ = n_try_;
  if (txn_weight_.size() != 2)
    get_micro_bench_read_req(req, cid);
  else
    switch (RandomGenerator::weighted_select(txn_weight_)) {
      case 0: // read
        get_micro_bench_read_req(req, cid);
        break;
      case 1: // write
        get_micro_bench_write_req(req, cid);
        break;
    }

}

void TxnGenerator::GetTxnReq(TxnRequest *req, uint32_t cid) {
  switch (benchmark_) {
    case TPCA:
    case TPCC:
    case TPCC_DIST_PART:
    case TPCC_REAL_DIST_PART:
      // should be called in sub-classes.
      verify(0);
      break;
    case MICRO_BENCH:
      get_micro_bench_txn_req(req, cid);
      break;
    default:
      Log_fatal("benchmark not implemented");
      verify(0);
  }
}

void TxnGenerator::get_txn_types(
    std::map<int32_t, std::string> &txn_types) {
  txn_types.clear();
  switch (benchmark_) {
    case TPCA:
      txn_types[TPCA_PAYMENT] = std::string(TPCA_PAYMENT_NAME);
      break;
    case TPCC:
    case TPCC_DIST_PART:
    case TPCC_REAL_DIST_PART:
      txn_types[TPCC_NEW_ORDER] = std::string(TPCC_NEW_ORDER_NAME);
      txn_types[TPCC_PAYMENT] = std::string(TPCC_PAYMENT_NAME);
      txn_types[TPCC_STOCK_LEVEL] = std::string(TPCC_STOCK_LEVEL_NAME);
      txn_types[TPCC_DELIVERY] = std::string(TPCC_DELIVERY_NAME);
      txn_types[TPCC_ORDER_STATUS] = std::string(TPCC_ORDER_STATUS_NAME);
      break;
    case RW_BENCHMARK:
      txn_types[RW_BENCHMARK_W_TXN] = std::string(RW_BENCHMARK_W_TXN_NAME);
      txn_types[RW_BENCHMARK_R_TXN] = std::string(RW_BENCHMARK_R_TXN_NAME);
      break;
    case MICRO_BENCH:
      txn_types[MICRO_BENCH_R] = std::string(MICRO_BENCH_R_NAME);
      txn_types[MICRO_BENCH_W] = std::string(MICRO_BENCH_W_NAME);
      break;
    default:
      Log_fatal("benchmark not implemented");
      verify(0);
  }
}

TxnGenerator::~TxnGenerator() {
}

} // namespace rcc
