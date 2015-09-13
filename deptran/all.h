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
#include <cstdint>

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
#include <boost/algorithm/string.hpp>

// yaml-cpp
#include <yaml-cpp/yaml.h>

// c library
#include <cstdio>
#include <cstdlib>
#include <cassert>
#include <cinttypes>

// google library

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
#include "memdb/utils.h"
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
#include "compress.hpp"
#include "constants.hpp"
#include "commo.h"
#include "config.h"
#include "txn-info.hpp"
#include "rcc_row.hpp"
#include "ro6_row.hpp"
#include "brq-common.h"
#include "brq-row.h"
#include "graph.hpp"
#include "rcc_rpc.h"
#include "dep_graph.hpp"
#include "brq-graph.h"
#include "dtxn.hpp"
#include "txn_reg.hpp"
#include "rcc.hpp"
#include "brq.h"
#include "marshal-value.h"
#include "rcc_service.h"
#include "txn_chopper.h"
#include "coordinator.h"
#include "brq-coord.h"
#include "sharding.h"
#include "timer.h"
#include "benchmark_control_rpc.h"
#include "piece.hpp"
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
