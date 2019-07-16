#!/usr/bin/env python
# encoding: ISO8859-1

"""
Copyright (c)2011, Hideyuki Tanaka

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.

    * Redistributions in binary form must reproduce the above
      copyright notice, this list of conditions and the following
      disclaimer in the documentation and/or other materials provided
      with the distribution.

    * Neither the name of Hideyuki Tanaka nor the names of other
      contributors may be used to endorse or promote products derived
      from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

import os, subprocess, sys
from waflib.TaskGen import before, after, feature
from waflib import Options, Task, Utils, Logs, Errors

C1 = '#XXX'.encode()
C2 = '#YYY'.encode()
UNPACK_DIR = '.unittest-gtest'
GTEST_DIR = 'gtest-1.7.0/fused-src'

def cleanup():
    import shutil
    try: shutil.rmtree(UNPACK_DIR)
    except OSError: pass

def unpack_gtest(conf):
    cwd = os.getcwd()

    fname = __file__
    if fname.endswith('.pyc'):
        fname = fname[0:-1]
    if fname.find('unittest_gtest.py') >= 0:
        fname = fname.replace('unittest_gtest.py', 'unittest_gtest.pygz2')
    print('fname:',fname)
    f = open(fname, 'rb')

    while 1:
        line = f.readline()
        if not line:
            Logs.error('not contain gtest archive')
            sys.exit(1)
        if line == '#==>\n'.encode():
            txt = f.readline()
            if not txt:
                Logs.error('corrupt archive')
            if f.readline() != '#<==\n'.encode():
                Logs.error('corrupt archive')
            break

    txt = txt[1:-1].replace(C1, '\n'.encode()).replace(C2, '\r'.encode())

    cleanup()

    tmp = 't.tar.bz2'

    os.makedirs(UNPACK_DIR)
    os.chdir(UNPACK_DIR)
    t = open(tmp, 'wb')
    t.write(txt)
    t.close()

    def check_call(args):
        if subprocess.call(args):
            raise

    try:
        check_call(['tar',  'xf', tmp])
        check_call(['mkdir', GTEST_DIR + '/gtest/gtest'])
        check_call(['cp', GTEST_DIR + '/gtest/gtest.h', GTEST_DIR + '/gtest/gtest/gtest.h'])
    except:
        os.chdir(cwd)
        cleanup()
        Logs.error('gtest cannot be unpacked.')

    os.unlink(tmp)
    conf.env.UNITTEST_GTEST_PATH = os.path.abspath(os.getcwd())
    os.chdir(cwd)

def configure(conf):
    try:
        unpack_gtest(conf)
        conf.msg('Unpacking gtest', 'yes')
    except:
        conf.msg('Unpacking gtest', 'no')
        Logs.error(sys.exc_info()[1])

    conf.check_cxx(lib = 'pthread', uselib_store = 'GTEST_PTHREAD')

def options(opt):
    opt.add_option('--check', action = 'store_true', default = False,
                   help = 'Execute unit tests')
    opt.add_option('--checkall', action = 'store_true', default = False,
                   help = 'Execute all unit tests')
    opt.add_option('--checkone', action = 'store', default = False,
                   help = 'Execute specified unit test')
    opt.add_option('--checkfilter', action = 'store', default = False,
                   help = 'Execute unit tests sprcified by pattern')
    opt.add_option('--checkleak', action = 'store_true', default = False,
                   help = 'Execute unit tests with valgrind')

def match_filter(filt, targ):
    if isinstance(filt, str):
        (pat, _, _) = filt.partition('.')
        if pat == '*':
            return True
        return pat == targ
    return False

@feature('testt', 'gtest')
@before('process_rule')
def test_remover(self):
    if not Options.options.check and not Options.options.checkall and self.target != Options.options.checkone and not match_filter(Options.options.checkfilter, self.target) and not Options.options.checkleak:
        self.meths[:] = []

@feature('gtest')
@before('process_source')
def gtest_attach(self):
    if not hasattr(self.bld, 'def_gtest_objects'):
        self.bld.objects(
          source = [UNPACK_DIR + '/' + GTEST_DIR + '/gtest/gtest-all.cc',
                    UNPACK_DIR + '/' + GTEST_DIR + '/gtest/gtest_main.cc'],
          target = 'GTEST_OBJECTS'
          )
        self.bld.def_gtest_objects = True

    print(self.env.UNITTEST_GTEST_PATH, GTEST_DIR)
    DIR = self.env.UNITTEST_GTEST_PATH + '/' + GTEST_DIR
    self.includes = self.to_list(getattr(self, 'includes', [])) + [DIR]
    self.use = self.to_list(getattr(self, 'use', [])) + ['GTEST_PTHREAD', 'GTEST_OBJECTS']

@feature('testt', 'gtest')
@after('apply_link')
def make_test(self):
    if not 'cprogram' in self.features and not 'cxxprogram' in self.features:
        Logs.error('test cannot be executed %s'%self)
        return
    self.default_install_path = None
    self.create_task('utest', self.link_task.outputs)

import threading
testlock = threading.Lock()

class utest(Task.Task):
    """
    Execute a unit test
    """
    color = 'PINK'
    after = ['vnum','inst']
    ext_in = ['.bin']
    vars = []
    def runnable_status(self):
        stat = super(utest, self).runnable_status()
        if stat != Task.SKIP_ME:
            return stat

        if Options.options.checkall or Options.options.checkleak:
            return Task.RUN_ME
        if Options.options.checkone == self.generator.name:
            return Task.RUN_ME
        if isinstance(Options.options.checkfilter, str):
            if match_filter(Options.options.checkfilter, self.generator.name):
                return Task.RUN_ME

        return stat

    def run(self):
        """
        Execute the test. The execution is always successful, but the results
        are stored on ``self.generator.bld.utest_results`` for postprocessing.
        """

        status = 0

        filename = self.inputs[0].abspath()
        self.ut_exec = getattr(self, 'ut_exec', [filename])
        if getattr(self.generator, 'ut_fun', None):
            self.generator.ut_fun(self)

        try:
            fu = getattr(self.generator.bld, 'all_test_paths')
        except AttributeError:
            fu = os.environ.copy()

            lst = []
            for g in self.generator.bld.groups:
                for tg in g:
                    if getattr(tg, 'link_task', None):
                        lst.append(tg.link_task.outputs[0].parent.abspath())

            def add_path(dct, path, var):
                dct[var] = os.pathsep.join(Utils.to_list(path) + [os.environ.get(var, '')])

            if sys.platform == 'win32':
                add_path(fu, lst, 'PATH')
            elif sys.platform == 'darwin':
                add_path(fu, lst, 'DYLD_LIBRARY_PATH')
                add_path(fu, lst, 'LD_LIBRARY_PATH')
            else:
                add_path(fu, lst, 'LD_LIBRARY_PATH')
            self.generator.bld.all_test_paths = fu


        if isinstance(Options.options.checkfilter, str):
            (_, _, filt) = Options.options.checkfilter.partition('.')
            if filt != "":
                self.ut_exec += ['--gtest_filter=' + filt]

        if Options.options.checkleak:
            self.ut_exec.insert(0, 'valgrind')
            self.ut_exec.insert(1, '--error-exitcode=1')
            self.ut_exec.insert(2, '--leak-check=full')
            self.ut_exec.insert(3, '--show-reachable=yes')

        cwd = getattr(self.generator, 'ut_cwd', '') or self.inputs[0].parent.abspath()
        proc = Utils.subprocess.Popen(self.ut_exec, cwd=cwd, env=fu, stderr=Utils.subprocess.STDOUT, stdout=Utils.subprocess.PIPE)
        (output, _) = proc.communicate()

        tup = (filename, proc.returncode, output)
        self.generator.utest_result = tup

        testlock.acquire()
        try:
            bld = self.generator.bld
            Logs.debug("ut: %r", tup)
            try:
                bld.utest_results.append(tup)
            except AttributeError:
                bld.utest_results = [tup]

            a = getattr(self.generator.bld, 'added_post_fun', False)
            if not a:
                self.generator.bld.add_post_fun(summary)
                self.generator.bld.added_post_fun = True

        finally:
            testlock.release()

def summary(bld):
    lst = getattr(bld, 'utest_results', [])

    if not lst: return

    total = len(lst)
    fail = len([x for x in lst if x[1]])

    Logs.pprint('CYAN', 'test summary')
    Logs.pprint('CYAN', '  tests that pass %d/%d' % (total-fail, total))

    for (f, code, out) in lst:
        if not code:
            Logs.pprint('GREEN', '    %s' % f)
            if isinstance(Options.options.checkfilter, str):
                print(out)

    if fail>0:
        Logs.pprint('RED', '  tests that fail %d/%d' % (fail, total))
        for (f, code, out) in lst:
            if code:
                Logs.pprint('RED', '    %s' % f)
                print(out.decode('utf-8'))
        raise Errors.WafError('test failed')
