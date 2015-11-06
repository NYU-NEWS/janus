cmake_minimum_required(VERSION 2.8.4)
project(rococo)
set(CMAKE_VERBOSE_MAKEFILE on)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Weffc++")

find_package(Threads)
find_package(PkgConfig REQUIRED)
pkg_check_modules(APR REQUIRED apr-1)
pkg_check_modules(APRUTIL REQUIRED apr-util-1)
pkg_check_modules(PYTHON2 REQUIRED python2)
pkg_check_modules(YAML_CPP REQUIRED yaml-cpp)

FIND_PACKAGE(Boost COMPONENTS system filesystem REQUIRED)
INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIR})

#set(CMAKE_CXX_FLAGS_DEBUG "-DLOG_DEBUG")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DLOG_LEVEL_AS_DEBUG")

#FIND_LIBRARY (DMALLOC_LIBRARY dmalloc /usr/lib /sw/lib)
#FIND_PATH (DMALLOC_INCLUDE_DIR dmalloc.h PATHS /usr/include /sw/include)
#IF(DMALLOC_LIBRARY)
#  ADD_DEFINITIONS(-D_USE_DMALLOC)
#  SET(LINK_TO_DMALLOC 1)
#ENDIF(DMALLOC_LIBRARY)


include_directories(
    ${PYTHON2_INCLUDE_DIRS}
    ${APR_INCLUDE_DIRS}
    ${YAML_CPP_INCLUDE_DIRS}
    .
    rrr
    memdb
    deptran
    deptran/tpl
    deptran/rcc
    deptran/ro6
    deptran/brq
    deptran/none
#    bench
#    bench/micro
#    bench/rw_benchmark
#    bench/tpca
#    bench/tpcc
#    bench/tpcc_dist
#    bench/tpcc_real_dist
)

link_directories(
    ${PYTHON2_LIBRARY_DIRS}
    ${APR_LIBRARY_DIRS}
    ${YAML_CPP_LIBRARY_DIRS}
)


#set (CMAKE_SHARED_LINKER_FLAGS "-Wl, -lyaml-cpp")


file(GLOB RRR_SRC
    "rrr/base/*.hpp"
    "rrr/base/*.cpp"
    "rrr/misc/*.hpp"
    "rrr/misc/*.cpp"
    "rrr/mq/*.cpp"
    "rrr/mq/*.h"
    "rrr/pylib/simplerpc/_pyrpc.cpp"
    "rrr/rpc/*.cpp"
    "rrr/rpc/*.hpp"
    "rrr/utils/*.h"
    "rrr/rrr.h"
    "rrr/rrr.hpp"
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
    "deptran/none/*.h"
    "deptran/none/*.cc"
)


add_library(ROCOCO
    ${RRR_SRC}
    ${MEMDB_SRC}
    ${BENCH_SRC}
    ${DEPTRAN_SRC}
)

add_executable(ROCOCO_SERVER
    deptran/s_main.cc
    ${RRR_SRC}
    )
#add_executable(ROCOCO_CLIENT deptran/c_main.cc)

target_link_libraries(
    ROCOCO_SERVER
    ROCOCO
    ${CMAKE_THREAD_LIBS_INIT}
    ${YAML_CPP_LIBRARIES}
    ${Boost_LIBRARIES}
    ${PYTHON2_LIBRARIES}
    ${APR_LIBRARIES}
    ${APRUTIL_LIBRARIES}
)
#
#target_link_libraries(
#    ROCOCO_CLIENT
#    ROCOCO
#    ${CMAKE_THREAD_LIBS_INIT}
#    ${YAML_CPP_LIBRARIES}
#
#)

#add_executable(ROCOCO_SERVER, deptran/s_main.cc)
#add_executable(ROCOCO_CLIENT ${SOURCE_FILES})
