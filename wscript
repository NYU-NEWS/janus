#!/usr/bin/env python

APPNAME="deptran"
VERSION="0.0"

import os
import sys
from waflib import Logs
from waflib import Options

pargs = ['--cflags', '--libs']

def options(opt):
    opt.load("compiler_cxx")
    #opt.load("eclipse")
    opt.add_option('-g', '--use-gxx', dest='cxx', 
                   default=False, action='store_true')
    opt.add_option('-c', '--use-clang', dest='clang', 
                   default=False, action='store_true')
    opt.add_option('-p', '--enable-profiling', dest='prof', 
                   default=False, action='store_true')
    opt.add_option('-d', '--debug', dest='debug', 
                   default=False, action='store_true')
    opt.add_option('-t', '--enable-tcmalloc', dest='tcmalloc', 
                   default=False, action='store_true')
    opt.add_option('-s', '--enable-rpc-statistics', dest='rpc_s', 
                   default=False, action='store_true')
    opt.add_option('-P', '--enable-piece-count', dest='pc', 
                   default=False, action='store_true')
    opt.add_option('-C', '--enable-conflict-count', dest='cc', 
                   default=False, action='store_true')
    opt.add_option('-r', '--enable-logging', dest='log', 
                   default=False, action='store_true')
    opt.add_option('-T', '--enable-txn-stat', dest='txn_stat', 
                   default=False, action='store_true')
    opt.parse_args();

def configure(conf):
    _choose_compiler(conf)
    conf.load("compiler_cxx")
    conf.load("python")
    conf.check_python_headers()

    _enable_tcmalloc(conf)
    _enable_pic(conf)
    _enable_cxx11(conf)
    _enable_debug(conf)
    _enable_profile(conf)
    _enable_rpc_s(conf)
    _enable_piece_count(conf)
    _enable_txn_count(conf)
    _enable_conflict_count(conf)
    #_enable_logging(conf)


    conf.env.append_value("CXXFLAGS", "-Wno-sign-compare")

    conf.check_cfg(package='apr-1', uselib_store='APR', args=pargs)
    conf.check_cfg(package='apr-util-1', uselib_store='APR-UTIL', args=pargs)
#    conf.check_cxx(lib='boost')

    conf.env.append_value("CXXFLAGS", "-Wno-sign-compare")
    conf.env.append_value('INCLUDES', ['/usr/local/include'])

    conf.env.append_value("CXXFLAGS", "-Wno-sign-compare")
    conf.env.LIB_PTHREAD = 'pthread'

    if sys.platform != 'darwin':
        conf.env.LIB_RT = 'rt'


def build(bld):
    _depend("rrr/pylib/simplerpcgen/rpcgen.py", 
            "rrr/pylib/simplerpcgen/rpcgen.g", 
            "rrr/pylib/yapps/main.py rrr/pylib/simplerpcgen/rpcgen.g")

#    _depend("rlog/log_service.h", "rlog/log_service.rpc", 
#            "bin/rpcgen rlog/log_service.rpc")

    _depend("deptran/rcc_rpc.h deptran/rcc_rpc.py", 
            "deptran/rcc_rpc.rpc", 
            "bin/rpcgen --python --cpp deptran/rcc_rpc.rpc")

    _depend("test/benchmark_service.h", "test/benchmark_service.rpc", 
            "bin/rpcgen --cpp test/benchmark_service.rpc")

    bld.stlib(source=bld.path.ant_glob("rrr/base/*.cpp "
                                       "rrr/misc/*.cpp "
                                       "rrr/rpc/*.cpp"), 
              target="rrr", 
              includes=". rrr", 
              use="PTHREAD APR APR-UTIL")

#    bld.stlib(source=bld.path.ant_glob("rpc/*.cc"), target="simplerpc", 
#              includes=". rrr rpc", 
#              use="base PTHREAD APR APR-UTIL")

    bld.stlib(source=bld.path.ant_glob("memdb/*.cc"), target="memdb", 
              includes=". rrr deptran base", 
              use="rrr PTHREAD APR APR-UTIL")

#    bld.stlib(source="rlog/rlog.cc", target="rlog", 
#              includes=". rrr rlog rpc", 
#              use="simplerpc base PTHREAD")

    bld.shlib(features="pyext", 
              source=bld.path.ant_glob("rrr/pylib/simplerpc/*.cc"), 
              target="_pyrpc", 
              includes=". rrr rrr/rpc", 
              use="rrr simplerpc PYTHON")

#    bld.program(source=bld.path.ant_glob("rlog/*.cc", excl="rlog/rlog.cc"), 
#                target="rlogserver", 
#                includes=". rrr", 
#                use="base simplerpc PTHREAD")

    bld.program(source=bld.path.ant_glob("test/test*.cc"), 
                target="testharness", 
                includes=". rrr deptran", 
                use="rrr memdb deptran PTHREAD RT")

    bld.program(source=bld.path.ant_glob("deptran/s_main.cc"), 
                target="deptran_server", 
                includes=". rrr deptran", 
                use="rrr memdb deptran PTHREAD PROFILER RT")

    bld.program(source=bld.path.ant_glob("deptran/c_main.cc"), 
                target="deptran_client", 
                includes=". rrr deptran", 
                use="rrr memdb deptran PTHREAD RT")

#    bld.program(source="test/rpcbench.cc test/benchmark_service.cc", 
#                target="rpcbench", 
#                includes=". rrr deptran test", 
#                use="rrr PTHREAD")
#
#    bld.program(source="test/rpc_microbench.cc test/benchmark_service.cc", 
#                target="rpc_microbench", 
#                includes=". rrr deptran test", 
#                use="rrr PTHREAD RT APR APR-UTIL")

    bld.stlib(source=bld.path.ant_glob("deptran/*.cc "
                                       "deptran/*.cpp "
                                       "deptran/util/*.cc "
                                       "deptran/tpca/*.cc deptran/tpcc/*.cc "
                                       "deptran/tpcc_real_dist/*.cc "
                                       "deptran/tpcc_dist/*.cc "
                                       "deptran/rw_benchmark/*.cc "
                                       "deptran/micro/*.cc"), 
              target="deptran", 
              includes=". rrr memdb deptran", 
              use="PTHREAD APR APR-UTIL base simplerpc memdb")

#
# waf helper functions
#

def _choose_compiler(conf):
    # use clang++ as default compiler (for c++11 support on mac)
    if sys.platform == 'darwin' and "CXX" not in os.environ:
        os.environ["CXX"] = "clang++"
    if Options.options.cxx:
        os.environ["CXX"] = "g++"
    elif Options.options.clang:
        os.environ["CXX"] = "clang++"

def _enable_rpc_s(conf):
    if Options.options.rpc_s:
        conf.env.append_value("CXXFLAGS", "-DRPC_STATISTICS")
        Logs.pprint("PINK", "RPC statistics enabled")

def _enable_piece_count(conf):
    if Options.options.pc:
        conf.env.append_value("CXXFLAGS", "-DPIECE_COUNT")
        Logs.pprint("PINK", "Piece count enabled")

def _enable_txn_count(conf):
    if Options.options.txn_stat:
        conf.env.append_value("CXXFLAGS", "-DTXN_STAT")
        Logs.pprint("PINK", "Txn stat enabled")

def _enable_conflict_count(conf):
    if Options.options.cc:
        conf.env.append_value("CXXFLAGS", "-DCONFLICT_COUNT")
        Logs.pprint("PINK", "Conflict count enabled")

def _enable_logging(conf):
    #if Options.options.log:
    conf.check(compiler='cxx', lib='aio', mandatory=True, uselib_store="AIO")
    conf.env.append_value("CXXFLAGS", "-DRECORD")
    conf.env.append_value("LINKFLAGS", "-laio")
    Logs.pprint("PINK", "Logging enabled")

def _enable_tcmalloc(conf):
    if Options.options.tcmalloc:
        Logs.pprint("PINK", "tcmalloc enabled")
        conf.env.append_value("LINKFLAGS", "-Wl,--no-as-needed")
        conf.env.append_value("LINKFLAGS", "-ltcmalloc")
        conf.env.append_value("LINKFLAGS", "-Wl,--as-needed")

def _enable_pic(conf):
    conf.env.append_value("CXXFLAGS", "-fPIC")
    conf.env.append_value("LINKFLAGS", "-fPIC")

def _enable_cxx11(conf):
    Logs.pprint("PINK", "C++11 features enabled")
    if sys.platform == "darwin":
        conf.env.append_value("CXXFLAGS", "-stdlib=libc++")
        conf.env.append_value("LINKFLAGS", "-stdlib=libc++")
    conf.env.append_value("CXXFLAGS", "-std=c++0x")

def _enable_profile(conf):
    if Options.options.prof:
        Logs.pprint("PINK", "CPU profiling enabled")
        conf.env.append_value("CXXFLAGS", "-DCPU_PROFILE")
        conf.env.LIB_PROFILER = 'profiler'

def _enable_debug(conf):
    if Options.options.debug:
        Logs.pprint("PINK", "Debug support enabled")
        conf.env.append_value("CXXFLAGS", "-Wall -pthread -O0 -DNDEBUG -g -ggdb -DLOG_DEBUG -rdynamic -fno-omit-frame-pointer".split())
    else:
        conf.env.append_value("CXXFLAGS", "-Wall -pthread -O2 -DNDEBUG".split())

def _properly_split(args):
    if args == None:
        return []
    else:
        return args.split()

def _depend(target, source, action):
    target = _properly_split(target)
    source = _properly_split(source)
    for s in source:
        if not os.path.exists(s):
            Logs.pprint('RED', "'%s' not found!" % s)
            exit(1)
    for t in target:
        if not os.path.exists(t):
            _run_cmd(action)
    if not target or min([os.stat(t).st_mtime for t in target]) < max([os.stat(s).st_mtime for s in source]):
        _run_cmd(action)

def _run_cmd(cmd):
    Logs.pprint('PINK', cmd)
    os.system(cmd)
