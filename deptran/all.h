#ifndef ALL_H_
#define ALL_H_


//#include "__dep__.h"
#include "marshal-value.h"


// rpc library

class dummy_class {
 public:
  dummy_class() {
#ifdef LOG_LEVEL_AS_DEBUG
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
//#include "rcc_rpc.h"
#include "txn_chopper.h"
#include "dep_graph.h"
#include "dtxn.h"
#include "rcc.h"
#include "rcc_service.h"
#include "coordinator.h"
#include "sharding.h"
#include "benchmark_control_rpc.h"
#include "piece.h"
#include "txn_req_factory.h"
#include "batch_start_args_helper.h"

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

#include "txn-chopper-factory.h"


#endif
