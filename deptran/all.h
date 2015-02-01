#ifndef ALL_H_
#define ALL_H_

//C++ standard library
#include <map>
#include <unordered_map>
#include <mutex>
#include <string>
#include <vector>
#include <chrono>
#include <thread>
#include <iostream>
#include <condition_variable>
#include <atomic>

using std::map;
using std::unordered_map;
using std::string;
using std::vector;


// system library
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

// boost library
#include <boost/any.hpp>
#include <boost/foreach.hpp>

// c library
#include <cstdio>
#include <cstdlib>
#include <cassert>
#include <cinttypes>

// misc helper files
#include "rrr.hpp"

using namespace rrr;

#define deptran rococo

using rrr::NoCopy;
using rrr::Log;
using rrr::i8;
using rrr::i16;
using rrr::i32;
using rrr::i64;

// User include files
//

#include "memdb/value.h"
#include "memdb/schema.h"
#include "memdb/table.h"
#include "memdb/txn.h"
using mdb::Value;

// rpc library

class dummy_class {
public:
    dummy_class () {
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
#include "constants.hpp"
#include "config.h"
#include "pie_info.h"
#include "txn_info.h"
#include "dep_row.h"
#include "txn_info.h"
#include "graph.hpp"
#include "rcc_rpc.h"
#include "dtxn.h"
#include "txn_reg.hpp"
#include "rcc.hpp"
#include "dep_graph.hpp"
#include "marshal-value.h"
#include "rcc_service.h"
#include "txn_chopper.h"
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
