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

//TxnGenerator *TxnGenerator::txn_req_factory_s = NULL;

TxnGenerator::TxnGenerator(Config* config)
    : txn_weight_(config->get_txn_weight()),
      txn_weights_(config->get_txn_weights()),
      sharding_(config->sharding_) {
  benchmark_ = Config::GetConfig()->get_benchmark();
  n_try_ = Config::GetConfig()->get_max_retry();
  single_server_ = Config::GetConfig()->get_single_server();

  std::map<std::string, uint64_t> table_num_rows;
  sharding_->get_number_rows(table_num_rows);

  switch (benchmark_) {
    case MICRO_BENCH:
      micro_bench_para_.n_table_a_ = table_num_rows[std::string(MICRO_BENCH_TABLE_A)];
      micro_bench_para_.n_table_b_ = table_num_rows[std::string(MICRO_BENCH_TABLE_B)];
      micro_bench_para_.n_table_c_ = table_num_rows[std::string(MICRO_BENCH_TABLE_C)];
      micro_bench_para_.n_table_d_ = table_num_rows[std::string(MICRO_BENCH_TABLE_D)];
      break;
    case TPCA:
      tpca_para_.n_branch_ = table_num_rows[std::string(TPCA_BRANCH)];
      tpca_para_.n_teller_ = table_num_rows[std::string(TPCA_TELLER)];
      tpca_para_.n_customer_ = table_num_rows[std::string(TPCA_CUSTOMER)];
      switch (single_server_) {
        case Config::SS_DISABLED:
          fix_id_ = -1;
          break;
        case Config::SS_THREAD_SINGLE:
        case Config::SS_PROCESS_SINGLE: {
          verify(tpca_para_.n_branch_ == tpca_para_.n_teller_
                     && tpca_para_.n_branch_ == tpca_para_.n_customer_);
          fix_id_ = RandomGenerator::rand(0, tpca_para_.n_branch_ - 1);
          unsigned int b, t, c;
          sharding_->GetPartition(TPCA_BRANCH, Value(fix_id_), b);
          sharding_->GetPartition(TPCA_TELLER, Value(fix_id_), t);
          sharding_->GetPartition(TPCA_CUSTOMER, Value(fix_id_), c);
          verify(b == c && c == t);
          break;
        }
        default:
          verify(0);
      }
      break;
    case TPCC:
    case TPCC_DIST_PART: {
//      std::vector<unsigned int> sites;
//      sharding_->get_site_id_from_tb(TPCC_TB_WAREHOUSE, sites);
//      uint64_t tb_w_rows = table_num_rows[std::string(TPCC_TB_WAREHOUSE)];
//      tpcc_para_.n_w_id_ = (int) tb_w_rows * sites.size();
//      tpcc_para_.const_home_w_id_ = RandomGenerator::rand(0, tpcc_para_.n_w_id_ - 1);
//      uint64_t tb_d_rows = table_num_rows[std::string(TPCC_TB_DISTRICT)];
//      tpcc_para_.n_d_id_ = (int) tb_d_rows / tb_w_rows;
//      uint64_t tb_c_rows = table_num_rows[std::string(TPCC_TB_CUSTOMER)];
//      tpcc_para_.n_c_id_ = (int) tb_c_rows / tb_d_rows;
//      tpcc_para_.n_i_id_ = (int) table_num_rows[std::string(TPCC_TB_ITEM)];
//      tpcc_para_.delivery_d_id_ = RandomGenerator::rand(0, tpcc_para_.n_d_id_ - 1);
//      if (single_server_ != Config::SS_DISABLED) {
//        verify(0);
//      }
//      else
//        fix_id_ = -1;
//      break;
    }
    case TPCC_REAL_DIST_PART: {
//      std::vector<unsigned int> sites;
//      sharding_->get_site_id_from_tb(TPCC_TB_WAREHOUSE, sites);
//      uint64_t tb_w_rows = table_num_rows[std::string(TPCC_TB_WAREHOUSE)];
//      tpcc_para_.n_w_id_ = (int) tb_w_rows;
//      tpcc_para_.const_home_w_id_ = RandomGenerator::rand(0, tpcc_para_.n_w_id_ - 1);
//      uint64_t tb_d_rows = table_num_rows[std::string(TPCC_TB_DISTRICT)];
//      tpcc_para_.n_d_id_ = (int) tb_d_rows * sites.size() / tb_w_rows;
//      uint64_t tb_c_rows = table_num_rows[std::string(TPCC_TB_CUSTOMER)];
//      tpcc_para_.n_c_id_ = (int) tb_c_rows / tb_d_rows;
//      tpcc_para_.n_i_id_ = (int) table_num_rows[std::string(TPCC_TB_ITEM)];
//      tpcc_para_.delivery_d_id_ = RandomGenerator::rand(0, tpcc_para_.n_d_id_ - 1);
//      switch (single_server_) {
//        case Config::SS_DISABLED:
//          fix_id_ = -1;
//          break;
//        case Config::SS_THREAD_SINGLE:
//        case Config::SS_PROCESS_SINGLE: {
//          fix_id_ = Config::GetConfig()->get_client_id() % tpcc_para_.n_d_id_;
//          tpcc_para_.const_home_w_id_ =
//              Config::GetConfig()->get_client_id() / tpcc_para_.n_d_id_;
//          break;
//        }
//        default:
//          verify(0);
//      }
      break;
    }
    case RW_BENCHMARK:
      rw_benchmark_para_.n_table_ = table_num_rows[std::string(RW_BENCHMARK_TABLE)];
      break;
    default:
      Log_fatal("benchmark not implemented");
      verify(0);
  }
}

void TxnGenerator::get_rw_benchmark_w_txn_req(
    TxnRequest *req, uint32_t cid) const {
  req->txn_type_ = RW_BENCHMARK_W_TXN;
  req->input_ = {
      {0, Value((i32) RandomGenerator::rand(0, rw_benchmark_para_.n_table_ - 1))},
      {1, Value((i32) RandomGenerator::rand(0, 10000))}
  };
}

void TxnGenerator::get_rw_benchmark_r_txn_req(
    TxnRequest *req, uint32_t cid) const {
  req->txn_type_ = RW_BENCHMARK_R_TXN;
  req->input_ = {
      {0, Value((i32) RandomGenerator::rand(0, rw_benchmark_para_.n_table_ - 1))}
  };
}

void TxnGenerator::get_rw_benchmark_txn_req(
    TxnRequest *req, uint32_t cid) const {
  req->n_try_ = n_try_;
  std::vector<double> weights = { txn_weights_["read"], txn_weights_["write"] };
  switch (RandomGenerator::weighted_select(weights)) {
    case 0: // read
      get_rw_benchmark_r_txn_req(req, cid);
      break;
    case 1: // write
      get_rw_benchmark_w_txn_req(req, cid);
      break;
    default:
      verify(0);
  }
}

void TxnGenerator::get_tpca_txn_req(
    TxnRequest *req, uint32_t cid) const {
  Value amount((i64) RandomGenerator::rand(0, 10000));
  req->n_try_ = n_try_;
  req->txn_type_ = TPCA_PAYMENT;
  if (fix_id_ >= 0) {
    int fix_tid = fix_id_;
    if (single_server_ == Config::SS_THREAD_SINGLE) {
      unsigned long s = (unsigned long) pthread_self();
      unsigned int s2 = (unsigned int) s;
      int r = rand_r(&s2);
      fix_tid += r;
      fix_tid = fix_tid >= 0 ? fix_tid : -fix_tid;
      fix_tid %= tpca_para_.n_branch_;
    }
    req->input_ = {
        {0, Value((i32) fix_tid)},
        {1, Value((i32) fix_tid)},
        {2, Value((i32) fix_tid)},
        {3, amount}
    };
  } else {
    req->input_ = {
        {0, Value((i32) RandomGenerator::rand(0, tpca_para_.n_customer_ - 1))},
        {1, Value((i32) RandomGenerator::rand(0, tpca_para_.n_teller_ - 1))},
        {2, Value((i32) RandomGenerator::rand(0, tpca_para_.n_branch_ - 1))},
        {3, amount}
    };
  }

}

void TxnGenerator::get_micro_bench_read_req(TxnRequest *req, uint32_t cid) const {
  req->txn_type_ = MICRO_BENCH_R;
//  req->input_.resize(4);
  req->input_[0] = Value((i32) RandomGenerator::rand(0, micro_bench_para_.n_table_a_ - 1));
  req->input_[1] = Value((i32) RandomGenerator::rand(0, micro_bench_para_.n_table_b_ - 1));
  req->input_[2] = Value((i32) RandomGenerator::rand(0, micro_bench_para_.n_table_c_ - 1));
  req->input_[3] = Value((i32) RandomGenerator::rand(0, micro_bench_para_.n_table_d_ - 1));
}

void TxnGenerator::get_micro_bench_write_req(TxnRequest *req, uint32_t cid) const {
  req->txn_type_ = MICRO_BENCH_W;
//  req->input_.resize(8);
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

void TxnGenerator::get_txn_req(
    TxnRequest *req, uint32_t cid) const {
  switch (benchmark_) {
    case TPCA:
      get_tpca_txn_req(req, cid);
      break;
    case TPCC:
    case TPCC_DIST_PART:
    case TPCC_REAL_DIST_PART:
//      get_tpcc_txn_req(req, cid);
      verify(0);
      break;
    case RW_BENCHMARK:
      get_rw_benchmark_txn_req(req, cid);
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
//
//void TxnGenerator::init_txn_req(TxnRequest *req, uint32_t cid) {
//  if (txn_req_factory_s == NULL)
//    txn_req_factory_s = new TxnGenerator();
//  if (req)
//    return txn_req_factory_s->get_txn_req(req, cid);
//}
//
//void TxnGenerator::destroy() {
//  if (txn_req_factory_s != NULL) {
//    delete txn_req_factory_s;
//    txn_req_factory_s = NULL;
//  }
//}

TxnGenerator::~TxnGenerator() {
}

} // namespace rcc
