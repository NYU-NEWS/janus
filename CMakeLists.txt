cmake_minimum_required(VERSION 2.8.4)
project(rococo)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

find_package(Threads)
find_package(PkgConfig REQUIRED)
pkg_check_modules(APR REQUIRED apr-1)
pkg_check_modules(PYTHON2 REQUIRED python2)
pkg_check_modules(YAMLCPP REQUIRED yaml-cpp)
#IF(APPLE)
#ELSE()
#ENDIF()

link_directories(
        ${PYTHON2_LIBRARY_DIRS}
        ${APR_LIBRARY_DIRS}
        ${YAMLCPP_LIBRARY_DIRS}
)

include_directories(
        ${PYTHON2_INCLUDE_DIRS}
        ${APR_INCLUDE_DIRS}
        ${YAMLCPP_INCLUDE_DIRS}
        .
        rrr
        memdb
        deptran
        deptran/tpl
        deptran/rcc
        deptran/ro6
        deptran/brq
        bench
        bench/micro
        bench/rw_benchmark
        bench/tpca
        bench/tpcc
        bench/tpcc_dist
        bench/tpcc_real_dist
)

file(GLOB MEMDB_SRC
        "memdb/*.h"
        "memdb/*.cc")

file(GLOB BENCH_SRC
        "bench/micro/*.h"
        "bench/micro/*.cc"
        "bench/rw_benchmark/*.cc"
        "bench/rw_benchmark/*.h"
        "bench/tpca/*.h"
        "bench/tpca/*.cc"
        "bench/tpcc/*.cc"
        "bench/tpcc/*.h"
        "bench/tpcc_dist/*.cc"
        "bench/tpcc_dist/*.h"
        "bench/tpcc_real_dist/*.cc"
        "bench/tpcc_real_dist/*.h"
)

file(GLOB DEPTRAN_SRC
        "deptran/*.h"
        "deptran/*.cc"
        "deptran/tpl/*.h"
        "deptran/tpl/*.cc"
        "deptran/rcc/*.h"
        "deptran/rcc/*.cc"
        "deptran/ro6/*.h"
        "deptran/ro6/*.cc"
        "deptran/brq/*.h"
        "deptran/brq/*.cc"
)

add_library(ROCOCO
        rrr/base/all.hpp
        rrr/base/basetypes.cpp
        rrr/base/basetypes.hpp
        rrr/base/debugging.cpp
        rrr/base/debugging.hpp
        rrr/base/logging.cpp
        rrr/base/logging.hpp
        rrr/base/misc.cpp
        rrr/base/misc.hpp
        rrr/base/strop.cpp
        rrr/base/strop.hpp
        rrr/base/threading.cpp
        rrr/base/threading.hpp
        rrr/base/unittest.cpp
        rrr/base/unittest.hpp
        rrr/misc/alarm.hpp
        rrr/misc/alock.cpp
        rrr/misc/alock.hpp
        rrr/misc/cpuinfo.hpp
        rrr/misc/dball.hpp
        rrr/misc/marshal.cpp
        rrr/misc/marshal.hpp
        rrr/misc/rand.cpp
        rrr/misc/rand.hpp
        rrr/misc/recorder.cpp
        rrr/misc/recorder.hpp
        rrr/misc/stat.hpp
        rrr/mq/buf.cpp
        rrr/mq/buf.h
        rrr/mq/client.cpp
        rrr/mq/client.h
        rrr/mq/polling.cpp
        rrr/mq/polling.h
        rrr/mq/rpc.cpp
        rrr/mq/rpc.h
        rrr/mq/rpc_comm.h
        rrr/mq/server.cpp
        rrr/mq/server.h
        rrr/pylib/simplerpc/_pyrpc.cpp
        rrr/rpc/client.cpp
        rrr/rpc/client.hpp
        rrr/rpc/polling.cpp
        rrr/rpc/polling.hpp
        rrr/rpc/server.cpp
        rrr/rpc/server.hpp
        rrr/rpc/utils.cpp
        rrr/rpc/utils.hpp
        rrr/utils/hostname.h
        rrr/utils/logger.h
        rrr/utils/mht.h
        rrr/utils/mlock.h
        rrr/utils/mpr_dag.h
        rrr/utils/mpr_hash.h
        rrr/utils/mpr_queue.h
        rrr/utils/mpr_thread_pool.h
        rrr/utils/mtime.h
        rrr/utils/safe_assert.h
        rrr/rrr.h
        rrr/rrr.hpp
        ${MEMDB_SRC}
        ${BENCH_SRC}
        ${DEPTRAN_SRC}
)

add_executable(ROCOCO_SERVER deptran/s_main.cc)
add_executable(ROCOCO_CLIENT deptran/c_main.cc)

target_link_libraries(
    ROCOCO_SERVER
    ROCOCO
    ${CMAKE_THREAD_LIBS_INIT}
    )

target_link_libraries(
    ROCOCO_CLIENT
    ROCOCO
    ${CMAKE_THREAD_LIBS_INIT}
    )

#add_executable(ROCOCO_SERVER, deptran/s_main.cc)
#add_executable(ROCOCO_CLIENT ${SOURCE_FILES})
