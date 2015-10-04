#ifndef ALL_H_
#define ALL_H_


#include "__dep__.h"

// rpc library

class dummy_class {
 public:
  dummy_class() {
#ifdef LOG_DEBUG
    Log::set_level(Log::DEBUG);
#else
    Log::set_level(Log::INFO);
#endif
  }
};
static dummy_class dummy___;


#define level_s

// User include files
#include "compress.h"
#include "constants.h"
#include "config.h"
#include "txn-info.h"
#include "rcc_row.h"
#include "ro6_row.h"
#include "graph.h"
#include "graph_marshaler.h"
#include "rcc_rpc.h"
#include "txn_chopper.h"
#include "dep_graph.h"
#include "dtxn.h"
#include "txn_reg.h"
#include "rcc.h"
#include "marshal-value.h"
#include "rcc_service.h"
#include "coordinator.h"
#include "sharding.h"
#include "timer.h"
#include "benchmark_control_rpc.h"
#include "piece.h"
#include "txn_req_factory.h"
#include "batch_start_args_helper.h"

// for tpca benchmark
#include "tpca/piece.h"
#include "tpca/chopper.h"

// tpcc benchmark
#include "tpcc/piece.h"
#include "tpcc/chopper.h"

// tpcc dist partition benchmark
#include "tpcc_dist/piece.h"
#include "tpcc_dist/chopper.h"

// tpcc real dist partition benchmark
#include "tpcc_real_dist/piece.h"
#include "tpcc_real_dist/chopper.h"

// rw benchmark
#include "rw_benchmark/piece.h"
#include "rw_benchmark/chopper.h"

// micro bench
#include "micro/piece.h"
#include "micro/chopper.h"

#include "txn-chopper-factory.h"


#endif
