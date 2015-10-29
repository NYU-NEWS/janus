#!/usr/bin/env python

# python system modules
import os
import sys
import time
import shutil
import logging
import subprocess
import multiprocessing
from optparse import OptionParser
from multiprocessing import Value
from multiprocessing import Lock
import xml.etree.ElementTree as ET
import yaml

# third-party python modules 
from tabulate import tabulate

# deptran python modules
sys.path += os.path.abspath(os.path.join(os.path.split(__file__)[0], "./rrr/pylib")),
sys.path += os.path.abspath(os.path.join(os.path.split(__file__)[0], "./deptran")),
from simplerpc import Client
from simplerpc.marshal import Marshal
from deptran.rcc_rpc import ServerControlProxy
from deptran.rcc_rpc import ClientControlProxy


cwd = os.getcwd()
deptran_home, ff = os.path.split(os.path.realpath(__file__))
g_log_dir = deptran_home + "/log"

g_latencies_percentage = [0.5, 0.9, 0.99, 0.999]
g_latencies_header = [str(x * 100) + "% LATENCY" for x in g_latencies_percentage]
g_att_latencies_percentage = [0.5, 0.9, 0.99, 0.999]
g_att_latencies_header = [str(x * 100) + "% ATT_LT" for x in g_att_latencies_percentage]
g_n_try_percentage = [0.5, 0.9, 0.99, 0.999]
g_n_try_header = [str(x * 100) + "% N_TRY" for x in g_n_try_percentage]
g_interest_txn = "NEW ORDER"
g_max_latency = 99999.9
g_max_try = 99999.9

hosts_path_g = ""
hosts_map_g = dict()

class TxnInfo(object):
    def __init__(self, txn_type, txn_name, interest):
        self.txn_type = txn_type
        self.start_txn = 0
        self.pre_start_txn = 0
        self.total_txn = 0
        self.pre_total_txn = 0
        self.total_try = 0
        self.pre_total_try = 0
        self.commit_txn = 0
        self.pre_commit_txn = 0
        self.txn_name = txn_name
        self.max_data = list()
        self.max_interval = 0.0
        self.interest = interest
        self.last_interval_start = 0
        self.this_latencies = []
        self.last_latencies = []
        self.attempt_latencies = []
        self.n_try = []
        self.min_interval = 0.0
        self.update_latency = False

        self.mid_status = 0 # 0: not started, 1: ongoing, 3: end
        self.mid_pre_start_txn = 0
        self.mid_start_txn = 0
        self.mid_pre_total_txn = 0
        self.mid_total_txn = 0
        self.mid_pre_total_try = 0
        self.mid_total_try = 0
        self.mid_pre_commit_txn = 0
        self.mid_commit_txn = 0
        self.mid_time = 0.0
        self.mid_latencies = []
        self.mid_attempt_latencies = []
        self.mid_n_try = []

    def set_mid_status(self):
        self.mid_status += 1

    def clear(self):
        self.start_txn = 0
        self.total_txn = 0
        self.total_try = 0
        self.commit_txn = 0
        self.this_latencies = []
        self.min_interval = g_max_latency
        self.attempt_latencies = []
        self.n_try = []

        if self.mid_status == 0:
            self.mid_pre_start_txn = 0
            self.mid_pre_total_txn = 0
            self.mid_pre_total_try = 0
            self.mid_pre_commit_txn = 0
        elif self.mid_status == 1:
            self.mid_start_txn = 0
            self.mid_total_txn = 0
            self.mid_total_try = 0
            self.mid_commit_txn = 0

    def push_res(self, start_txn, total_txn, total_try, commit_txn, 
            this_latencies, last_latencies, latencies, 
            attempt_latencies, interval_time, n_tried):
        self.start_txn += start_txn
        self.total_txn += total_txn
        self.total_try += total_try
        self.commit_txn += commit_txn
        #self.this_latencies.extend(this_latencies)
        #self.last_latencies.extend(last_latencies)
        if self.min_interval > interval_time:
            self.min_interval = interval_time
        #self.attempt_latencies.extend(attempt_latencies)
        #self.n_try.extend(n_tried)

        if self.mid_status == 0:
            self.mid_pre_start_txn += start_txn
            self.mid_pre_total_txn += total_txn
            self.mid_pre_total_try += total_try
            self.mid_pre_commit_txn += commit_txn
        elif self.mid_status == 1:
            self.mid_latencies.extend(latencies)
            self.mid_attempt_latencies.extend(attempt_latencies)
            self.mid_time += interval_time
            self.mid_n_try.extend(n_tried)
            self.mid_start_txn += start_txn
            self.mid_total_txn += total_txn
            self.mid_total_try += total_try
            self.mid_commit_txn += commit_txn

    def get_res(self, interval_time, total_time, set_max, 
            all_total_commits, all_interval_commits, do_sample, do_sample_lock):
        #self.last_latencies.sort()
        min_latency = g_max_latency
        max_latency = g_max_latency
        #self.last_latencies.extend([self.min_interval*1000.0] * (self.last_interval_start - len(self.last_latencies)))
        latencies_size = len(self.last_latencies)
        #if (latencies_size > 0):
        #    min_latency = self.last_latencies[0]
        #    max_latency = self.last_latencies[latencies_size - 1]
        #latencies_sample_size = [int(x * latencies_size) for x in g_latencies_percentage]
        #interval_latencies = []
        interval_latencies = [g_max_latency for x in g_latencies_percentage]
        #for s_size in latencies_sample_size:
        #    if s_size != 0:
        #        interval_latencies.append(sum(self.last_latencies[0:s_size]) / s_size)
        #    else:
        #        interval_latencies.append(g_max_latency)

        #self.attempt_latencies.sort()
        #attempt_latencies_size = len(self.attempt_latencies)
        #attempt_latencies_sample_size = [int(x * attempt_latencies_size) for x in g_att_latencies_percentage]
        #interval_attempt_latencies = []
        interval_attempt_latencies = [g_max_latency for x in g_att_latencies_percentage]
        #for s_size in attempt_latencies_sample_size:
        #    if s_size != 0:
        #        interval_attempt_latencies.append(sum(self.attempt_latencies[0:s_size]) / s_size)
        #    else:
        #        interval_attempt_latencies.append(g_max_latency)

        #self.n_try.sort()
        #n_try_size = len(self.n_try)
        #n_try_sample_size = [int(x * n_try_size) for x in g_n_try_percentage]
        #int_n_try = []
        int_n_try = [g_max_try for x in g_n_try_percentage]
        #for s_size in n_try_sample_size:
        #    if s_size != 0:
        #        int_n_try.append(sum(self.n_try[0:s_size]) * 1.0 / s_size)
        #    else:
        #        int_n_try.append(g_max_try)

        interval_tps = int(round((self.commit_txn - self.pre_commit_txn) / interval_time))

        interval_commits = self.commit_txn - self.pre_commit_txn

        if all_total_commits > 0:
            total_ret = [str(round(self.commit_txn * 100.0 / all_total_commits, 2)) + "%", self.txn_name, self.start_txn, self.total_txn, self.total_try, self.commit_txn, int(round(self.commit_txn / total_time))]
        else:
            total_ret = ["----", self.txn_name, self.start_txn, self.total_txn, self.total_try, self.commit_txn, int(round(self.commit_txn / total_time))]

        if all_interval_commits > 0:
            interval_ret = [str(round(interval_commits * 100.0 / all_interval_commits, 2)) + "%", self.txn_name, self.start_txn - self.pre_start_txn, self.total_txn - self.pre_total_txn, self.total_try - self.pre_total_try, interval_commits, interval_tps, min_latency, max_latency]
        else:
            interval_ret = ["----", self.txn_name, self.start_txn - self.pre_start_txn, self.total_txn - self.pre_total_txn, self.total_try - self.pre_total_try, interval_commits, interval_tps, min_latency, max_latency]

        interval_ret.extend(interval_latencies)
        interval_ret.extend(interval_attempt_latencies)
        interval_ret.extend(int_n_try)
        ret = [total_ret, interval_ret]

        if (self.update_latency):
            self.update_latency = False
            ul_index = 9
            while ul_index < len(ret[1]):
                self.max_data[ul_index] = ret[1][ul_index]
                ul_index += 1

        if (set_max):
            if (len(self.max_data) == 0 or self.max_data[6] < interval_tps):
                self.max_data = ret[1]
                self.max_interval = interval_time
                self.update_latency = True
                #if self.interest:
                    #do_sample_lock.acquire()
                    #do_sample.value = 1
                    #do_sample_lock.release()

        self.last_interval_start = self.start_txn - self.pre_start_txn
        self.pre_start_txn = self.start_txn
        self.pre_total_txn = self.total_txn
        self.pre_total_try = self.total_try
        self.pre_commit_txn = self.commit_txn
        #self.last_latencies = self.this_latencies #XXX
        self.this_latencies = []
        return ret

    def print_mid(self, num_clients):
        start_txn = str(self.mid_start_txn - self.mid_pre_start_txn)
        total_txn = str(self.mid_total_txn - self.mid_pre_total_txn)
        tries = str(self.mid_total_try - self.mid_pre_total_try)
        commit_txn = str(self.mid_commit_txn - self.mid_pre_commit_txn)
        self.mid_time /= num_clients
        tps = str(int(round((self.mid_commit_txn - self.mid_pre_commit_txn) / self.mid_time)))

        self.mid_latencies.sort()
        self.mid_attempt_latencies.sort()
        self.mid_n_try.sort()

        min_latency = g_max_latency
        max_latency = g_max_latency
        latency_str = ""
        latencies_size = len(self.mid_latencies)
        if (latencies_size > 0):
            min_latency = self.mid_latencies[0]
            max_latency = self.mid_latencies[latencies_size - 1]
        latencies_sample_size = [int(x * latencies_size) for x in g_latencies_percentage]
        i = 0
        while i < len(g_latencies_header):
            latency_str += "; " + g_latencies_header[i] + ": "
            s_size = latencies_sample_size[i]
            if s_size != 0:
                latency_str += str(sum(self.mid_latencies[0:s_size]) / s_size)
            else:
                latency_str += str(g_max_latency)
            i += 1

        attempt_latencies_size = len(self.mid_attempt_latencies)
        attempt_latencies_sample_size = [int(x * attempt_latencies_size) for x in g_att_latencies_percentage]
        i = 0
        while i < len(g_att_latencies_header):
            latency_str += "; " + g_att_latencies_header[i] + ": "
            s_size = attempt_latencies_sample_size[i]
            if s_size != 0:
                latency_str += str(sum(self.mid_attempt_latencies[0:s_size]) / s_size)
            else:
                latency_str += str(g_max_latency)
            i += 1

        n_tried_str = ""
        n_try_size = len(self.mid_n_try)
        n_try_sample_size = [int(x * n_try_size) for x in g_n_try_percentage]
        i = 0
        while i < len(g_n_try_header):
            n_tried_str += "; " + g_n_try_header[i] + ": "
            s_size = n_try_sample_size[i]
            if s_size != 0:
                n_tried_str += str(sum(self.mid_n_try[0:s_size]) * 1.0 / s_size)
            else:
                n_tried_str += str(g_max_try)
            i += 1

        print "RECORDING_RESULT: TXN: <" + self.txn_name + ">; STARTED_TXNS: " + start_txn + "; FINISHED_TXNS: " + total_txn + "; ATTEMPTS: " + tries + "; COMMITS: " + commit_txn + "; TPS: " + tps + latency_str + "; TIME: " + str(self.mid_time) + "; LATENCY MIN: " + str(min_latency) + "; LATENCY MAX: " + str(max_latency) + n_tried_str

    def print_max(self):
        latency_str = ""
        i = 0
        for l_str in g_latencies_header:
            latency_str += "; " + l_str + ": " + str(self.max_data[9 + i])
            i += 1

        i = 0
        latency_size = len(g_latencies_header)
        for l_str in g_att_latencies_header:
            latency_str += "; " + l_str + ": " + str(self.max_data[9 + latency_size + i])
            i += 1

        n_tried_str = ""
        i = 0
        att_latency_size = len(g_att_latencies_header)
        for l_str in g_n_try_header:
            n_tried_str += "; " + l_str + ": " + str(self.max_data[9 + latency_size + att_latency_size + i])
            i += 1

        print "RECORDING_RESULT: TXN: <" + str(self.max_data[1]) + ">; STARTED_TXNS: " + str(self.max_data[2]) + "; FINISHED_TXNS: " + str(self.max_data[3]) + "; ATTEMPTS: " + str(self.max_data[4]) + "; COMMITS: " + str(self.max_data[5]) + "; TPS: " + str(self.max_data[6]) + latency_str + "; TIME: " + str(self.max_interval) + "; LATENCY MIN: " + str(self.max_data[7]) + "; LATENCY MAX: " + str(self.max_data[8]) + n_tried_str

class ClientController(object):
    def __init__(self, benchmark, timeout, c_info, duration, 
            single_server, taskset, log_dir, interest_txn, recording_path):
        self.print_max = False
        self.benchmark = benchmark
        self.timeout = timeout
        self.c_info = c_info
        self.duration = duration
        self.start_time = 0
        self.txn_infos = dict()
        self.finish_set = set()
        self.rpc_proxy = dict()
        self.pre_start_txn = 0
        self.start_txn = 0
        self.pre_total_txn = 0
        self.total_txn = 0
        self.pre_total_try = 0
        self.total_try = 0
        self.pre_commit_txn = 0
        self.commit_txn = 0
        self.run_sec = 0
        self.pre_run_sec = 0
        self.run_nsec = 0
        self.pre_run_nsec = 0
        self.n_asking = 0;
        self.single_server = str(single_server)
        self.machine_n_cores = dict()
        self.taskset = taskset
        self.log_dir = log_dir
        self.recording_period = False
        self.max_tps = 0
        self.max_data = list()
        self.txn_names = dict()
        self.interest_txn = interest_txn
        self.recording_path = recording_path

    def taskset_func(self, m_id):
        if (self.taskset):
            if (self.c_info[m_id][0] in self.machine_n_cores):
                m_info = self.machine_n_cores[self.c_info[m_id][0]]
                ret = m_info[0]
                m_info[0] += 1
                if (m_info[0] > m_info[1]):
                    m_info[0] = 0
                return "taskset -c " + str(ret)
            else:
                n_cores = subprocess.check_output('ssh ' + self.c_info[m_id][0] + ' "cat /proc/cpuinfo | grep processor | tail -n1 | sed \'s/.*processor\\s*:\\s*\\([0-9]\\+\\)/\\1/g\'"', shell=True)
                tmp_lst = [1, int(n_cores)]
                if (1 > tmp_lst[1]):
                    tmp_lst[0] = 0
                self.machine_n_cores[self.c_info[m_id][0]] = tmp_lst
                return "taskset -c 0"
        else:
            return ""

    def start(self, filename):
        i = 0
        while (i < len(self.c_info)):
            cmd = ""
            recording = ""
            if (len(self.recording_path) != 0):
                recording = " -r '" + self.recording_path + "/deptran_client_" + str(i) + "' "
                cmd += "mkdir -p '" + self.recording_path + "'; "
            cmd += "cd " + deptran_home + "; " \
                + " nohup " + self.taskset_func(i) + " ./build/deptran_client " \
                + " -c " + str(i) \
                + " -d " + str(self.duration) \
                + " -f " + filename \
                + " -p " + self.c_info[i][1] \
                + " -t " + str(self.timeout) \
                + " -H " + hosts_path_g \
                + " -S " + self.single_server \
                + " -b " \
                + recording \
                + " 1>\"" + self.log_dir + "/client-" + str(i) + ".log\"" \
                + " 2>\"" + self.log_dir + "/client-" + str(i) + ".err\"" \
                + " &"
            print cmd
            subprocess.call(['ssh', '-n', '-f', self.c_info[i][0], cmd])
            i += 1

    def client_run(self, do_sample, do_sample_lock):
        try:
            i = 0
            while (i < len(self.c_info)):
                client = Client()
                con = 1
                connect_start = time.time()
                while (con != 0):
                    con = client.connect(self.c_info[i][0] + ":" + self.c_info[i][1])
                    if time.time() - connect_start > self.timeout:
                        self.client_kill()
                        return
                    time.sleep(0.1)
                self.rpc_proxy[i] = ClientControlProxy(client)
                #print "Connected to client: " + self.c_info[i][0] + ":" + self.c_info[i][1]
                i += 1

            ready_futures = []
            i = 0
            while (i < len(self.rpc_proxy)):
                ready_futures.append(self.rpc_proxy[i].async_client_ready_block())
                i += 1
            i = 0
            while (i < len(ready_futures)):
                ready_futures[i].wait()
                i += 1

            res = self.rpc_proxy[0].sync_client_get_txn_names()
            for k, v in res.items():
                self.txn_names[k] = v

            print "Clients all ready"

            self.start_client()

            print "Clients started"

            self.benchmark_record(do_sample, do_sample_lock)
            self.client_shutdown()

        except:
            self.client_force_shutdown()
        print "Benchmark finished\n"

    def client_force_shutdown(self):
        print "Force clients shutdown ..."
        i = 0
        while (i < len(self.rpc_proxy)):
            try:
                self.rpc_proxy[i].sync_client_force_stop()
            except:
                pass
            i += 1
        try:
            self.client_shutdown()
        except:
            pass
        print "Clients shutdown"
        self.client_kill()
        print "Clients killed"

    def start_client(self):
        futures = []
        i = 0
        while (i < len(self.rpc_proxy)):
            futures.append(self.rpc_proxy[i].async_client_start())
            i += 1

        i = 0
        while (i < len(futures)):
            futures[i].wait()
            i += 1
        self.start_time = time.time()

    def benchmark_record(self, do_sample, do_sample_lock):
        while (len(self.rpc_proxy) != len(self.finish_set)):
            time.sleep(self.timeout)
            for k in self.txn_infos.keys():
                self.txn_infos[k].clear()
            self.start_txn = 0
            self.total_txn = 0
            self.total_try = 0
            self.commit_txn = 0
            self.run_sec = 0
            self.run_nsec = 0
            i = 0
            futures = []
            while (i < len(self.rpc_proxy)):
                futures.append(self.rpc_proxy[i].async_client_response())
                i += 1

            i = 0
            while (i < len(futures)):
                res = futures[i].result
                period_time = res.period_sec + res.period_nsec / 1000000000.0
                for txn_type in res.txn_info.keys():
                    if txn_type not in self.txn_infos:
                        self.txn_infos[txn_type] = TxnInfo(txn_type, self.txn_names[txn_type], self.txn_names[txn_type] == self.interest_txn)
                    self.start_txn += res.txn_info[txn_type].start_txn
                    self.total_txn += res.txn_info[txn_type].total_txn
                    self.total_try += res.txn_info[txn_type].total_try
                    self.commit_txn += res.txn_info[txn_type].commit_txn
                    self.txn_infos[txn_type].push_res(res.txn_info[txn_type].start_txn, res.txn_info[txn_type].total_txn, res.txn_info[txn_type].total_try, res.txn_info[txn_type].commit_txn, res.txn_info[txn_type].this_latency, res.txn_info[txn_type].last_latency, res.txn_info[txn_type].interval_latency, res.txn_info[txn_type].attempt_latency, period_time, res.txn_info[txn_type].num_try)
                self.run_sec += res.run_sec
                self.run_nsec += res.run_nsec
                self.n_asking += res.n_asking
                if (res.is_finish == 1):
                    self.finish_set.add(i)
                i += 1
            self.cur_time = time.time()
            need_break = self.print_stage_result(do_sample, do_sample_lock)
            if (need_break):
                break

    def print_stage_result(self, do_sample, do_sample_lock):
        interval_time = (self.run_sec - self.pre_run_sec + (self.run_nsec - self.pre_run_nsec) / 1000000000.0) / len(self.c_info)
        total_time = (self.run_sec + self.run_nsec / 1000000000.0) / len(self.c_info)
        progress = int(round(100 * total_time / self.duration))

        if (self.print_max):
            self.print_max = False
            for k, v in self.txn_infos.items():
                #v.print_max()
                v.print_mid(len(self.rpc_proxy))

        if (not self.recording_period):
            if (progress >= 20 and progress <= 60):
                self.recording_period = True
                do_sample_lock.acquire()
                do_sample.value = 1
                do_sample_lock.release()
                for k, v in self.txn_infos.items():
                    v.set_mid_status()
        else:
            if (progress >= 60):
                self.recording_period = False
                self.print_max = True
                do_sample_lock.acquire()
                do_sample.value = 1
                do_sample_lock.release()
                for k, v in self.txn_infos.items():
                    v.set_mid_status()
        output_str = "\nProgress: " + str(progress) + "%\n"
        total_table = []
        interval_table = []
        interval_commits = self.commit_txn - self.pre_commit_txn
        for txn_type in self.txn_infos.keys():
            rows = self.txn_infos[txn_type].get_res(interval_time, total_time, self.recording_period, self.commit_txn, interval_commits, do_sample, do_sample_lock)
            total_table.append(rows[0])
            interval_table.append(rows[1])
        total_table.append(["----", "Total", self.start_txn, self.total_txn, self.total_try, self.commit_txn, int(round(self.commit_txn / total_time))])
        interval_total_row = ["----", "Total", self.start_txn - self.pre_start_txn, self.total_txn - self.pre_total_txn, self.total_try - self.pre_total_try, interval_commits, int(round((self.commit_txn - self.pre_commit_txn) / interval_time))]
        interval_total_row.extend([0.0 for x in g_latencies_header])
        interval_total_row.extend([0.0 for x in g_att_latencies_header])
        interval_table.append(interval_total_row)
        total_header = ["RATIO", "NAME", "start", "finish", "attempts", "commits", "TPS"]
        interval_header = ["RATIO", "NAME", "start", "finish", "attempts", "commits", "TPS", "min lt", "max lt"]
        interval_header.extend(g_latencies_header)
        interval_header.extend(g_att_latencies_header)
        interval_header.extend(g_n_try_header)
        output_str += "TOTAL: elapsed time: " + str(round(total_time, 2)) + "\n"
        output_str += tabulate(total_table, headers=total_header) + "\n\n"
        output_str += "INTERVAL: elapsed time: " + str(round(interval_time, 2)) + "\n"
        output_str += tabulate(interval_table, headers=interval_header) + "\n"
        output_str += "\tTotal asking finish: " + str(self.n_asking) + "\n"
        output_str += "----------------------------------------------------------------------\n"
        print output_str

        self.pre_start_txn = self.start_txn
        self.pre_total_txn = self.total_txn
        self.pre_total_try = self.total_try
        self.pre_commit_txn = self.commit_txn
        self.pre_run_sec = self.run_sec
        self.pre_run_nsec = self.run_nsec

        if (self.cur_time - self.start_time > 1.5 * self.duration):
            if (self.print_max):
                self.print_max = False
                for k, v in self.txn_infos.items():
                    v.print_mid(len(self.rpc_proxy))
            return True
        else:
            return False

    def client_kill(self):
        kill_set = set()
        for v in self.c_info.values():
            kill_set.add(v[0])

        for v in kill_set:
            try:
                subprocess.call(['ssh', '-n', '-f', v, "killall -9 deptran_client &>/dev/null"])
            except:
                pass

    def client_shutdown(self):
        print "Shutting down clients ..."
        i = 0
        while (i < len(self.rpc_proxy)):
            try:
                self.rpc_proxy[i].sync_client_shutdown()
            except:
                pass
            i += 1

class ServerResponse(object):
    def __init__(self, value_times_pair):
        self.value = value_times_pair.value
        self.times = value_times_pair.times

    def add_one(self, value_times_pair):
        self.value += value_times_pair.value
        self.times += value_times_pair.times

    def get_value(self):
        return self.value

    def get_times(self):
        return self.times

    def get_ave(self):
        if self.times == 0:
            return 0.0
        else:
            return 1.0 * self.value / self.times

class ServerController(object):
    def __init__(self, timeout, s_info, taskset, log_dir, recording_path):
        self.timeout = timeout
        self.s_info = s_info
        self.rpc_proxy = dict()
        self.log_dir = log_dir

        if (taskset == 1):
            # set task on CPU 1
            self.taskset_func = lambda x: "taskset -c " + str(2 * x + 16)
            logging.info("Setting servers on CPU 1")
        elif (taskset == 2): 
            # set task on CPU 0, odd number cores, no overlapping with irq cores
            self.taskset_func = lambda x: "taskset -c " + str(2 * x + 1)
            logging.info("Setting servers on CPU 0, odd number cores")
        elif (taskset == 3): 
            # set task on CPU 0, even number cores, overlapping with irq cores
            self.taskset_func = lambda x: "taskset -c " + str(2 * x)
            logging.info("Setting servers on CPU 0, even number cores")
        else:
            self.taskset_func = lambda x: ""
            logging.info("No taskset, auto scheduling")

        self.pre_statistics = dict()
        self.pre_time = time.time()
        self.recording_path = recording_path

    def server_kill(self):
        kill_set = set()
        for v in self.s_info.values():
            kill_set.add(v[0])

        for v in kill_set:
            try:
                subprocess.call(['ssh', '-n', '-f', v, "killall -9 deptran_server &>/dev/null"])
            except:
                pass
        print "SERVERKILLED"

    def server_heart_beat(self, cond, s_init_finish, do_sample, do_sample_lock):
        try:
            i = 0
            while (i < len(self.s_info)):
                client = Client()
                con = 1
                connect_start = time.time()
                while (con != 0):
                    con = client.connect(self.s_info[i][0] + ":" + self.s_info[i][1])
                    if time.time() - connect_start > self.timeout:
                        self.server_kill()
                        exit(1)
                    time.sleep(0.1)
                self.rpc_proxy[i] = ServerControlProxy(client)
                #print "Connected to server: " + self.s_info[i][0] + ":" + self.s_info[i][1]
                i += 1

            i = 0
            while (i < len(self.rpc_proxy)):
                while (self.rpc_proxy[i].sync_server_ready() != 1):
                    time.sleep(1)# waiting for server to initialize
                i += 1

            cond.acquire()
            s_init_finish.value = 1
            cond.notify()
            cond.release()

            avg_r_cnt = 0.0
            avg_r_sz = 0.0
            avg_cpu_util = 0.0
            sample_result = []
            #timeout_counter = 0
            while (True):
                do_statistics = False
                do_sample_lock.acquire()
                if do_sample.value == 1:
                    do_statistics = True
                    do_sample.value = 0
                do_sample_lock.release()
                i = 0
                r_cnt_sum = 0
                r_cnt_num = 0
                r_sz_sum = 0
                r_sz_num = 0
                statistics = dict()
                cpu_util = [0.0] * len(self.rpc_proxy)
                futures = []
                while (i < len(self.rpc_proxy)):
                    if do_statistics:
                        futures.append(self.rpc_proxy[i].async_server_heart_beat_with_data())
                    else:
                        futures.append(self.rpc_proxy[i].async_server_heart_beat())
                    i += 1

                i = 0
                while (i < len(futures)):
                    #if timeout_counter == 4:
                    if do_statistics:
                        ret = futures[i].result
                        r_cnt_sum += ret.r_cnt_sum
                        r_cnt_num += ret.r_cnt_num
                        r_sz_sum += ret.r_sz_sum
                        r_sz_num += ret.r_sz_num
                        cpu_util[i] = ret.cpu_util
                        for k, v in ret.statistics.items():
                            if k not in statistics:
                                statistics[k] = ServerResponse(v)
                            else:
                                statistics[k].add_one(v)
                    else:
                        futures[i].wait()
                    i += 1
                #if timeout_counter == 4:
                #    timeout_counter = 0
                if do_statistics:
                    total_result = []
                    interval_result = []
                    cur_time = time.time()
                    interval_time = cur_time - self.pre_time
                    self.pre_time = cur_time
                    for k, v in statistics.items():
                        total_result.append([k, v.get_value(), v.get_times(), v.get_ave()])
                        #if k not in self.pre_statistics:
                        interval_result.append([k, v.get_value(), v.get_times(), v.get_ave(), interval_time])
                        #else:
                        #    pre_v = self.pre_statistics[k]
                        #    value_buf = v.get_value() - pre_v.get_value()
                        #    times_buf = v.get_times() - pre_v.get_times()
                        #    if times_buf == 0:
                        #        interval_result.append([k, value_buf, times_buf, 0.0, interval_time])
                        #    else:
                        #        interval_result.append([k, value_buf, times_buf, 1.0 * value_buf / times_buf, interval_time])
                    #print "\n=== SERVER STATISTICS: ===\nTOTAL:\n" + tabulate(total_result, headers=["Key", "Total Number", "Times", "Ave"]) + "\n\nINTERVAL:\n" + tabulate(interval_result, headers=["Key", "Total Number", "Times", "Ave", "Time"]) + "\n==========================\n"
                    self.pre_statistics = statistics
                    #do_sample_lock.acquire()
                    #if (do_sample.value == 1):
                    sample_result = interval_result
                    #    avg_cpu_util = sum(cpu_util) / len(cpu_util)

                    #    do_sample.value = 0
                    #do_sample_lock.release()
                    avg_cpu_util = sum(cpu_util) / len(cpu_util)
                    if r_cnt_num != 0:
                        avg_r_cnt = (1.0 * r_cnt_sum) / r_cnt_num
                    else:
                        avg_r_cnt = -1.0
                    if r_sz_num != 0:
                        avg_r_sz = (1.0 * r_sz_sum) / r_sz_num
                    else:
                        avg_r_sz = -1.0
                cond.acquire()
                if (s_init_finish.value == 0):
                    cond.release()
                    break
                cond.release()
                time.sleep(self.timeout / 4)
                #timeout_counter += 1

            for single_record in sample_result:
                print "SERVREC: " + str(single_record[0]) + ": VALUE: " + str(single_record[1]) + "; TIMES: " + str(single_record[2]) + "; MEAN: " + str(single_record[3]) + "; TIME: " + str(single_record[4])
            print "CPUINFO: " + str(avg_cpu_util) + ";"
            print "AVG_LOG_FLUSH_CNT: " + str(avg_r_cnt) + ";"
            print "AVG_LOG_FLUSH_SZ: " + str(avg_r_sz) + ";"
            print "BENCHMARK_SUCCEED"

            print "Shutting down servers ..."
            i = 0
            while (i < len(self.rpc_proxy)):
                try:
                    self.rpc_proxy[i].sync_server_shutdown()
                except:
                    pass
                i += 1
            time.sleep(1)
            self.server_kill()
        except:
            cond.acquire()
            s_init_finish.value = 5
            cond.notify()
            cond.release()
            print "Shutting down servers ..."
            i = 0
            while (i < len(self.rpc_proxy)):
                try:
                    self.rpc_proxy[i].sync_server_shutdown()
                except:
                    pass
                i += 1
            time.sleep(1)
            self.server_kill()

    def start(self, filename):
        i = 0
        machine_map = dict()
        while (i < len(self.s_info)):
            if (self.s_info[i][0] in machine_map):
                machine_no = machine_map[self.s_info[i][0]]
            else:
                machine_no = 0
            machine_map[self.s_info[i][0]] = machine_no + 1
            cmd = ""
            recording = ""
            if (len(self.recording_path) != 0):
                recording = " -r '" + self.recording_path + "/deptran_server_" + str(i) + "' "
                cmd += "mkdir -p '" + self.recording_path + "'; "
            cmd += "cd " + deptran_home + "; "
            cmd += "nohup " + self.taskset_func(machine_no) + " ./build/deptran_server " \
                + " -s " + str(i) \
                + " -f " + filename \
                + " -p " + self.s_info[i][1] \
                + " -H " + hosts_path_g \
                + " -t " + str(self.timeout) \
                + " -b " \
                + recording \
                + " 1>\"" + self.log_dir + "/site-" + str(i) + ".log\"" \
                + " 2>\"" + self.log_dir + "/site-" + str(i) + ".err\"" \
                + " &"
            print cmd
            subprocess.call(['ssh', '-n', '-f', self.s_info[i][0], cmd])
            i += 1

def init_hosts_map():
    global hosts_map_g
    f = open(hosts_path_g, "r")
    for line in f:
        [hostname, sitename] = line.split()
        hosts_map_g[sitename] = hostname

def site2host_name(sitename):
    if sitename in hosts_map_g:
        return hosts_map_g[sitename]
    else:
        return sitename

def worker2host(workername, config):
    if 'process' not in config:
        return workername
    workermap = config['process']
    if workername in workermap:
        return workermap[workername]
    else:
        return workername

def load_workers(site_type, config_obj, rpc_port, res):

    logging.info("loading %s information", site_type)

    if 'site' not in config_obj:
        logging.error("Configuration: no sites infomation.")
        return False

    sites = config_obj['site']

    if site_type not in sites:
        logging.error("Configuration: Didn't specify the %s", site_type)
        return False

    workers = sites[site_type]
        
    sidx = 0
    for partition in workers:
        for worker in partition:
            if ":" in worker:
                #Server configuration with 
                [workername, port] = worker.split(':')
            else:
                #Client configuration with 
                workername = worker

            hostname = worker2host(workername, config_obj)
            res[sidx] = tuple((hostname, str(rpc_port)))
            sidx += 1
            rpc_port += 1
            
    return True

def create_parser():
    
    parser = OptionParser()

    parser.add_option("-f", "--file", dest="config_path", 
            help="read config from FILE, default is properties.xml", 
            default="./config/tpccd-sample.xml", metavar="FILE")

    parser.add_option("-P", "--port", dest="rpc_port", help="port to use", 
            default=5555, metavar="PORT")

    parser.add_option("-t", "--server-timeout", dest="s_timeout", 
            help="server heart beat timeout in seconds", default=10, 
            action="store", metavar="TIMEOUT")

    parser.add_option("-i", "--status-time-interval", dest="c_timeout", 
            help="time interval to report benchmark status in seconds", 
            default=5, action="store", metavar="TIME")

    parser.add_option("-d", "--duration", dest="c_duration", 
            help="benchmark running duration in seconds", default=60, 
            action="store", metavar="TIME")

    parser.add_option("-S", "--single-server", dest="c_single_server", 
            help="control each client always touch the same server "
                 "0, disabled; 1, each thread will touch a single server; "
                 "2, each process will touch a single server", 
            default=0, action="store", metavar="[0|1|2]")

    parser.add_option("-T", "--taskset-schema", dest="s_taskset", 
            help="Choose which core to run each server on. "
                 "0: auto; "
                 "1: CPU 1; "
                 "2: CPU 0, odd cores; "
                 "3: CPU 0, even cores;", 
            default=0, action="store", metavar="[0|1|2|3]")

    parser.add_option("-c", "--client-taskset", dest="c_taskset", 
            help="taskset client processes round robin", default=False, 
            action="store_true")

    parser.add_option("-l", "--log-dir", dest="log_dir", 
            help="Log file directory", default=g_log_dir, 
            metavar="LOG_DIR")

    parser.add_option("-r", "--recording-path", dest="recording_path", 
            help="Recording path", default="", metavar="RECORDING_PATH")

    parser.add_option("-x", "--interest-txn", dest="interest_txn", 
            help="interest txn", default=g_interest_txn, 
            metavar="INTEREST_TXN")

    parser.add_option("-H", "--hosts", dest="hosts_path", 
            help="hosts path", default="./config/hosts-local", 
            metavar="HOSTS_PATH")
    
    return parser
    
class TrialConfig:
    def __init__(self, options):
        self.s_timeout = int(options.s_timeout)
        self.c_timeout = int(options.c_timeout)
        self.c_duration = int(options.c_duration)
        self.c_single_server = int(options.c_single_server)
        self.s_taskset = int(options.s_taskset)
        self.c_taskset = options.c_taskset
        self.config_path = os.path.realpath(options.config_path)
        self.hosts_path = os.path.realpath(options.hosts_path)
        self.recording_path = os.path.realpath(options.recording_path)
        self.log_path = os.path.realpath(options.log_dir)
        self.c_interest_txn = str(options.interest_txn)
        self.rpc_port = int(options.rpc_port)        
        
        pass
        
    def check_correctness(self):
        if self.c_single_server not in [0, 1, 2]:
            logging.error("Invalid single server argument.")
            return False
            
        if not os.path.exists(self.config_path):
            logging.error("Config path incorrect.")
            return False
        
        if not os.path.exists(self.hosts_path):
            logging.error("Hosts path incorrect.")
            return False
        
        return True

def main():
    logging.basicConfig(level=logging.INFO)
    
    try:
        # load command arguments into configuration
        parser = create_parser()
        (options, args) = parser.parse_args()
        global hosts_path_g
        s_timeout = int(options.s_timeout)
        c_timeout = int(options.c_timeout)
        c_duration = int(options.c_duration)
        c_single_server = int(options.c_single_server)
        s_taskset = int(options.s_taskset)
        c_taskset = options.c_taskset
        filename = os.path.realpath(options.config_path)
        hosts_path_g = os.path.realpath(options.hosts_path)
        log_dir = os.path.realpath(options.log_dir)
        # the kind of transaction I care about (new-order)
        c_interest_txn = str(options.interest_txn)
        # recording (recovery log) path  
        recording_path_dir = ""
        if (len(options.recording_path) != 0):
            recording_path_dir = os.path.realpath(options.recording_path)
        rpc_port = int(options.rpc_port)
        logging.info("Experiment controller port: " + str(options.rpc_port))

        if not TrialConfig(options).check_correctness():
            return False;

        init_hosts_map()

        # (debug) log path
        shutil.rmtree(log_dir, True)
        os.makedirs(log_dir)

        # the configuration xml file
        logging.info("Start reading config file: " + filename + " ...")
        config = ET.parse(filename).getroot()

        benchmark = config.attrib["name"]

        # TODO beautify the following code
        # 1. load server info
        # 2. load client info
        # 3. start server and setup heartbeat.
        # 4. wait until server init 
        # 5. start client and setup heartbeat.
        # 6. start bench
        # 7. collect results
        # 7. finish bench
        logging.info("Checking site info ...")
        s_info = dict()

        yml_filename = os.path.realpath("./config/sample.yml")
        stream = file(yml_filename, 'r')
        yaml_obj = yaml.load(stream)

        load_workers('server', yaml_obj, rpc_port, s_info)
        print s_info

        c_info = dict()
        load_workers('client', yaml_obj, rpc_port + len(s_info), c_info)
        print c_info
        # server_info = site_info['server']
        
        # sidx = 0
        # for partition in server_info:
        #     for server in partition:
        #         # print server
        #         [workername, port] = server.split(':')
        #         hostname = worker2host(workername, yaml_obj)
        #         s_info[sidx] = tuple((hostname, str(rpc_port)))
        #         sidx += 1
        #         rpc_port += 1
        #         print workername
        #         print hostname
        #         # print port
        # print s_info

        if 'client' not in site_info:
            logging.error("Configuration: Didn't specify the clients.")
            return False

        # print server_info
        return True
        # init server controller
        server_controller = ServerController(s_timeout, s_info, s_taskset, 
                log_dir, recording_path_dir)

        # start all server processes
        logging.info("Starting servers ...")
        server_controller.start(filename)
        logging.info("Starting servers ... Done")

        cond = multiprocessing.Condition()
        s_init_finish = Value('i', 0)

        do_sample = Value('i', 0)
        do_sample_lock = Lock()

        server_process = multiprocessing.Process(
                target=server_controller.server_heart_beat, 
                args=(cond, s_init_finish, do_sample, do_sample_lock))
        server_process.daemon = False
        server_process.start()

        logging.info("Waiting for server init ...")
        cond.acquire()
        while (s_init_finish.value == 0):
            cond.wait()
        if s_init_finish.value == 5:
            logging.error("Waiting for server init ... FAIL")
            server_process.join()
            server_controller.server_kill()
            return False
        cond.release()
        logging.info("Waiting for server init ... Done")

        #time.sleep(5);

        # init client controller
        client_controller = ClientController(benchmark, c_timeout, c_info, c_duration, 
                c_single_server, c_taskset, log_dir, c_interest_txn, recording_path_dir)

        logging.info("Starting clients ...")
        client_controller.start(filename)
        logging.info("Starting clients ... Done")

        # let all clients start running the benchmark
        client_controller.client_run(do_sample, do_sample_lock)
        cond.acquire()
        s_init_finish.value = 0
        cond.release()

        #server_process.terminate()
        server_process.join()
        server_controller.server_kill()
        client_controller.client_kill()

    except:
        try:
            server_process.terminate()
            server_process.join()
        except:
            pass
        try:
            server_controller.server_kill()
        except:
            pass
        try:
            client_controller.client_kill()
        except:
            pass

if __name__ == "__main__":
    main()

