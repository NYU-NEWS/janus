
#include "config.h"
#include "constants.h"
#include "sharding.h"

// for tpca benchmark
#include "bench/tpca/workload.h"
#include "bench/tpca/payment.h"

// tpcc benchmark
#include "bench/tpcc/workload.h"
#include "bench/tpcc/procedure.h"

// tpcc dist partition benchmark
#include "bench/tpcc_dist/procedure.h"

// tpcc real dist partition benchmark
#include "bench/tpcc_real_dist/workload.h"
#include "bench/tpcc_real_dist/procedure.h"

// rw benchmark
#include "bench/rw/workload.h"
#include "bench/rw/procedure.h"

// micro bench
#include "bench/micro/workload.h"
#include "bench/micro/procedure.h"

namespace janus {

Workload* Workload::CreateWorkload(Config *config) {
  switch (config->benchmark()) {
    case TPCA:
      return new TpcaWorkload(config);
    case TPCC:
      return new TpccWorkload(config);
    case TPCC_DIST_PART:
      verify(0);
      return new TpccWorkload(config);
    case TPCC_REAL_DIST_PART:
      return new TpccRdWorkload(config);
    case RW_BENCHMARK:
      return new RwWorkload(config);
    case MICRO_BENCH:
      return new MicroWorkload(config);
    default:
      verify(0);
      return NULL;
  }
}

Workload::Workload(Config* config)
    : txn_weight_(config->get_txn_weight()),
      txn_weights_(config->get_txn_weights()),
      sharding_(config->sharding_) {
  benchmark_ = Config::GetConfig()->benchmark();
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

void Workload::GetProcedureTypes(map<int32_t, string> &txn_types) {
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

Workload::~Workload() {
}

} // namespace janus

