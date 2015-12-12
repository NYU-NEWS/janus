#pragma once



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
#include <algorithm>
#include <set>

using std::map;
using std::unordered_map;
using std::string;
using std::vector;
using std::set;
using std::pair;

// system library
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>


// boost library
#include <boost/any.hpp>
#include <boost/foreach.hpp>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>

// yaml-cpp
#include <yaml-cpp/yaml.h>

// c library
#include <cstdio>
#include <cstdlib>
#include <cassert>
#include <cstring>
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
using rrr::Mutex;
using rrr::Future;
using rrr::DragonBall;
using rrr::RandomGenerator;
using rrr::Recorder;
using rrr::AvgStat;
using rrr::ALock;
using rrr::TimeoutALock;
using rrr::ScopedLock;
using rrr::PollMgr;
using rrr::Marshal;

// User include files
//

#include "memdb/value.h"
#include "memdb/schema.h"
#include "memdb/table.h"
#include "memdb/txn.h"
#include "memdb/txn_2pl.h"
#include "memdb/txn_occ.h"
#include "memdb/txn_unsafe.h"
#include "memdb/utils.h"
#include "memdb/row.h"
using mdb::Value;
