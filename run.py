#!/usr/bin/env python3

# python system modules
import os
import signal
import sys
import time
import shutil
import logging
import subprocess
import multiprocessing
import argparse
import traceback
import itertools
import random
import math
import signal
from threading import Thread
from argparse import ArgumentParser
from multiprocessing import Value
from multiprocessing import Lock
import yaml
import tempfile
from collections import OrderedDict

# third-party python modules
from tabulate import tabulate

# deptran python modules
sys.path += os.path.abspath(os.path.join(os.path.split(__file__)[0], "./src/rrr/pylib")),
sys.path += os.path.abspath(os.path.join(os.path.split(__file__)[0], "./src")),
from simplerpc import Client
from simplerpc.marshal import Marshal
from deptran.rcc_rpc import ServerControlProxy
from deptran.rcc_rpc import ClientControlProxy
from pylib import ps

if sys.version_info < (3, 0):
    sys.stdout.write("Sorry, requires Python 3.x, not Python 2.x\n")
    sys.exit(1)

g_exit = False
def signal_handler(sig, frame):
    print('You pressed Ctrl+C!')
    global g_exit
    g_exit = True
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

LOG_LEVEL = logging.DEBUG
LOG_FILE_LEVEL = logging.DEBUG
logger = logging.getLogger('janus')

cwd = os.getcwd()
deptran_home, ff = os.path.split(os.path.realpath(__file__))
g_log_dir = deptran_home + "/log"

ONE_BILLION = float(10 ** 9)
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
        self.all_latencies = []
        self.n_try = []
        self.min_interval = 0.0
        self.update_latency = False

        self.mid_retry_exhausted = 0
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
        self.mid_all_latencies = []
        self.mid_attempt_latencies = []
        self.mid_n_try = []

    def set_mid_status(self):
        self.mid_status += 1

    def clear(self):
        self.start_txn = 0
        self.total_txn = 0
        self.total_try = 0
        self.commit_txn = 0
        self.mid_retry_exhausted = 0
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
            attempt_latencies, interval_time, n_tried, n_retry_exhausted):
        self.start_txn += start_txn
        self.total_txn += total_txn
        self.total_try += total_try
        self.commit_txn += commit_txn
        if self.min_interval > interval_time:
            self.min_interval = interval_time

        if self.mid_status == 0:
            logger.debug("before recording period: {}".format(self.txn_type))
            self.mid_pre_start_txn += start_txn
            self.mid_pre_total_txn += total_txn
            self.mid_pre_total_try += total_try
            self.mid_pre_commit_txn += commit_txn
            logger.debug("mid_pre_commit_txn (+{}): {}".format(commit_txn, self.mid_pre_commit_txn))
        elif self.mid_status == 1:
            logger.debug("during recording period!!! {}".format(self.txn_type))
            self.mid_latencies.extend(latencies)
            self.mid_all_latencies.extend(self.mid_latencies)
            self.mid_attempt_latencies.extend(attempt_latencies)
            self.mid_time += interval_time
            self.mid_n_try.extend(n_tried)
            self.mid_start_txn += start_txn
            self.mid_total_txn += total_txn
            self.mid_total_try += total_try
            self.mid_commit_txn += commit_txn
            self.mid_retry_exhausted += n_retry_exhausted
            #logger.debug("mid_commit_txn (+{}): {}".format(commit_txn, self.mid_commit_txn))
            #logger.debug("self.mid_latencies: {}".format(self.mid_latencies))

    def get_res(self, interval_time, total_time, set_max,
            all_total_commits, all_interval_commits, do_sample, do_sample_lock):
        min_latency = g_max_latency
        max_latency = g_max_latency
        latencies_size = len(self.last_latencies)
        interval_latencies = [g_max_latency for x in g_att_latencies_percentage]
        interval_attempt_latencies = [g_max_latency for x in g_att_latencies_percentage]
        int_n_try = [g_max_try for x in g_n_try_percentage]

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

        self.last_interval_start = self.start_txn - self.pre_start_txn
        self.pre_start_txn = self.start_txn
        self.pre_total_txn = self.total_txn
        self.pre_total_try = self.total_try
        self.pre_commit_txn = self.commit_txn
        self.this_latencies = []
        return ret

    def print_mid(self, config, num_clients):
        start_txn = self.mid_start_txn - self.mid_pre_start_txn
        total_txn = self.mid_total_txn - self.mid_pre_total_txn
        tries = self.mid_total_try - self.mid_pre_total_try
        commit_txn = self.mid_commit_txn - self.mid_pre_commit_txn
        self.mid_time /= num_clients
        tps = commit_txn / self.mid_time

        logger.info("mid_commit_txn: {}".format(self.mid_commit_txn))
        logger.info("mid_pre_commit_txn: {}".format(self.mid_pre_commit_txn))
        logger.info("mid_time = {}".format(self.mid_time))

        if self.mid_retry_exhausted > 0:
            m = [0]
            if len(self.mid_latencies) > 0:
              m = [max(self.mid_latencies)]
            self.mid_all_latencies.extend(m * self.mid_retry_exhausted)

        self.mid_latencies.sort()
        self.mid_all_latencies.sort()
        self.mid_attempt_latencies.sort()
        self.mid_n_try.sort()

        NO_VALUE = 999999.99

        latencies = {}
        all_latencies = {}
        att_latencies = {}
        for percent in g_latencies_percentage:
            logger.info("percent: {}".format(percent))
            percent = percent*100
            key = str(percent)
            if len(self.mid_latencies)>0:
                index = int(math.ceil(percent/100*len(self.mid_latencies)))-1
                latencies[key] = self.mid_latencies[index]
            else:
                latencies[key] = NO_VALUE

            if len(self.mid_all_latencies)>0:
                index = int(math.ceil(percent/100*len(self.mid_all_latencies)))-1
                all_latencies[key] = self.mid_all_latencies[index]
            else:
                all_latencies[key] = NO_VALUE

            if len(self.mid_attempt_latencies)>0:
                att_index = int(math.ceil(percent/100*len(self.mid_attempt_latencies)))-1
                att_latencies[key] = self.mid_attempt_latencies[att_index]
            else:
                att_latencies[key] = NO_VALUE

        num_clients = sum([len(x)
                           for x in config['site']['client']]) * \
                      config["n_concurrent"]

        zipf = -1
        if 'coefficient' in config['bench']:
            zipf = config['bench']['coefficient']

        self.data = {
            'benchmark': config['bench']['workload'],
            'cc': config['mode']['cc'],
            'ab': config['mode']['ab'],
            'clients': num_clients,
            'duration': self.mid_time,
            'txn_name': self.txn_name,
            'start_cnt': start_txn,
            'total_cnt': total_txn,
            'attempts': tries,
            'commits': commit_txn,
            'tps': tps,
            'zipf': zipf,
            'experiment_id': int(config['args'].experiment_id),
            'retries_exhausted_cnt': self.mid_retry_exhausted
        }


        self.data['latency'] = {}
        self.data['latency'].update(latencies)
        if len(self.mid_latencies)>0:
            self.data['latency']['min'] = self.mid_latencies[0]
            self.data['latency']['max'] = self.mid_latencies[len(self.mid_latencies)-1]
        else:
            self.data['latency']['min'] = NO_VALUE
            self.data['latency']['max'] = NO_VALUE

        self.data['all_latency'] = {}
        self.data['all_latency'].update(all_latencies)
        if len(self.mid_all_latencies)>0:
            self.data['all_latency']['min'] = self.mid_all_latencies[0]
            self.data['all_latency']['max'] = self.mid_all_latencies[len(self.mid_all_latencies)-1]
        else:
            self.data['all_latency']['min'] = NO_VALUE
            self.data['all_latency']['max'] = NO_VALUE

        self.data['att_latency'] = {}
        self.data['att_latency'].update(att_latencies)
        if len(self.mid_latencies)>0:
            self.data['att_latency']['min'] = self.mid_attempt_latencies[0]
            self.data['att_latency']['max'] = self.mid_attempt_latencies[
                len(self.mid_attempt_latencies)-1
            ]
        else:
            self.data['att_latency']['min'] = NO_VALUE
            self.data['att_latency']['max'] = NO_VALUE

        logger.info("\n__Data__\n{}\n__EndData__\n".format(yaml.dump(self.data)))


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

        logger.info("RECORDING_RESULT: TXN: <" + str(self.max_data[1]) + ">; STARTED_TXNS: " + str(self.max_data[2]) + "; FINISHED_TXNS: " + str(self.max_data[3]) + "; ATTEMPTS: " + str(self.max_data[4]) + "; COMMITS: " + str(self.max_data[5]) + "; TPS: " + str(self.max_data[6]) + latency_str + "; TIME: " + str(self.max_interval) + "; LATENCY MIN: " + str(self.max_data[7]) + "; LATENCY MAX: " + str(self.max_data[8]) + n_tried_str)


class ClientController(object):
    def __init__(self, config, process_infos):
        self.config = config
        self.process_infos = process_infos
        self.benchmark = config['bench']['workload']
        self.timeout = config['args'].c_timeout
        self.duration = config['args'].c_duration
        self.taskset = config['args'].c_taskset
        self.log_dir = config['args'].log_dir
        self.interest_txn = config['args'].interest_txn
        self.recording_path = config['args'].recording_path

        self.max_data = list()
        self.finish_set = set()
        self.txn_infos = dict()
        self.rpc_proxy = dict()
        self.txn_names = dict()
        self.machine_n_cores = dict()

        self.start_time = 0
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
        self.n_asking = 0
        self.max_tps = 0

        self.recording_period = False
        self.print_max = False

    def client_run(self, do_sample, do_sample_lock):
        sites = ProcessInfo.get_sites(self.process_infos,
                                      SiteInfo.SiteType.Client)
        for site in sites:
            site.connect_rpc(30)
            logger.info("Connected to client site %s @ %s", site.name, site.process.host_address)

        barriers = []
        for site in sites:
            barriers.append(site.process.client_rpc_proxy.async_client_ready_block())

        for barrier in barriers:
            barrier.wait()
        logger.info("Clients all ready")

        res = sites[0].process.client_rpc_proxy.sync_client_get_txn_names()
        for k, v in res.items():
            logger.debug("txn: %s - %s", v, k)
            self.txn_names[k] = v.decode()

        self.start_client()
        logger.info("Clients started")

        start = time.time()
        try:
            self.benchmark_record(do_sample, do_sample_lock)
        finally:
            logger.info("Duration: {:.2f} seconds".format(time.time() - start))

        logger.info("Benchmark finished")

    def start_client(self):
        sites = ProcessInfo.get_sites(self.process_infos,
                    SiteInfo.SiteType.Client)
        client_rpc = set()
        for site in sites:
            client_rpc.add(site.process.client_rpc_proxy)

        futures = []
        for rpc_proxy in client_rpc:
            futures.append(rpc_proxy.async_client_start())

        for future in futures:
            future.wait()

        logger.info("client start send successfully.")

        self.start_time = time.time()

    def benchmark_record(self, do_sample, do_sample_lock):
        logger.debug("in benchmark_record")
        sites = ProcessInfo.get_sites(self.process_infos,
                                      SiteInfo.SiteType.Client)
        rpc_proxy = set()
        for site in sites:
            rpc_proxy.add(site.process.client_rpc_proxy)
        rpc_proxy = list(rpc_proxy)
        logger.info("contact {} client rpc proxies".format(len(rpc_proxy)))
        self.num_proxies = len(rpc_proxy)

        while (len(rpc_proxy) != len(self.finish_set)):
            logger.debug("top client heartbeat; timeout {}".format(self.timeout))
            for k in self.txn_infos.keys():
                self.txn_infos[k].clear()
            self.start_txn = 0
            self.total_txn = 0
            self.total_try = 0
            self.commit_txn = 0
            self.run_sec = 0
            self.run_nsec = 0

            futures = []
            for proxy in rpc_proxy:
                try:
                    future = proxy.async_client_response()
                    futures.append(future)
                except:
                    logger.error(traceback.format_exc())
            i=0
            for future in futures:
                res = future.result
                period_time = res.period_sec + res.period_nsec / ONE_BILLION
                for txn_type in res.txn_info.keys():
                    if txn_type not in self.txn_infos:
                        self.txn_infos[txn_type] = TxnInfo(txn_type, self.txn_names[txn_type], self.txn_names[txn_type] == self.interest_txn)
                    self.start_txn += res.txn_info[txn_type].start_txn
                    self.total_txn += res.txn_info[txn_type].total_txn
                    self.total_try += res.txn_info[txn_type].total_try
                    self.commit_txn += res.txn_info[txn_type].commit_txn

                    #print("num_exhausted: ", res.txn_info[txn_type].num_exhausted)
                    #print("num lat: ", len(res.txn_info[txn_type].attempt_latency))

                    self.txn_infos[txn_type].push_res(
                        res.txn_info[txn_type].start_txn,
                        res.txn_info[txn_type].total_txn,
                        res.txn_info[txn_type].total_try,
                        res.txn_info[txn_type].commit_txn,
                        res.txn_info[txn_type].this_latency,
                        res.txn_info[txn_type].last_latency,
                        res.txn_info[txn_type].interval_latency,
                        res.txn_info[txn_type].attempt_latency,
                        period_time,
                        res.txn_info[txn_type].num_try,
                        res.txn_info[txn_type].num_exhausted)

                logger.debug("timing from server: run_sec {:.2f}; run_nsec {:.2f}".format(res.run_sec, res.run_nsec))
                self.run_sec += res.run_sec
                self.run_nsec += res.run_nsec
                self.n_asking += res.n_asking
                if (res.is_finish == 1):
                    self.finish_set.add(i)
                i += 1

            if len(futures) == 0:
                logger.fatal("client control rpc futures length 0")
                sys.exit(1)

            self.run_sec /= len(futures)
            self.run_nsec /= len(futures)
            logger.debug("avg timing from {} servers: run_sec {:.2f}; run_nsec {:.2f}".format(len(futures), res.run_sec, res.run_nsec))

            self.cur_time = time.time()
            need_break = self.print_stage_result(do_sample, do_sample_lock)
            if (need_break):
                break
            else:
                time.sleep(self.timeout)

    def print_stage_result(self, do_sample, do_sample_lock):
        # sites = ProcessInfo.get_sites(self.process_infos,
        #                               SiteInfo.SiteType.Client)

        interval_time = (self.run_sec - self.pre_run_sec) + \
                        ((self.run_nsec - self.pre_run_nsec) / ONE_BILLION)

        total_time = (self.run_sec + self.run_nsec / ONE_BILLION)

        progress = int(round(100 * total_time / self.duration))
        logger.info("Progress: {}".format(progress))

        #if (self.print_max):
        #    self.print_max = False
        #    for k, v in self.txn_infos.items():
        #        #v.print_max()
        #        v.print_mid(self.config, self.num_proxies)

        lower_cutoff_pct = 25
        upper_cutoff_pct = 75

        if (not self.recording_period):
            if (progress >= lower_cutoff_pct and progress <= upper_cutoff_pct):
                logger.info("start recording period")
                self.recording_period = True
                do_sample_lock.acquire()
                do_sample.value = 1
                do_sample_lock.release()
                for k, v in self.txn_infos.items():
                    v.set_mid_status()
        else:
            if (progress >= upper_cutoff_pct):
                logger.info("done with recording period")
                self.recording_period = False

                for k, v in self.txn_infos.items():
                    v.print_mid(self.config, self.num_proxies)

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
        logger.info("total_time: {}".format(total_time))
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
        logger.info(output_str)

        self.pre_start_txn = self.start_txn
        self.pre_total_txn = self.total_txn
        self.pre_total_try = self.total_try
        self.pre_commit_txn = self.commit_txn
        self.pre_run_sec = self.run_sec
        self.pre_run_nsec = self.run_nsec

        if (self.cur_time - self.start_time > 1.5 * self.duration):
            #if (self.print_max):
            #    self.print_max = False
            #    for k, v in self.txn_infos.items():
            #        v.print_mid(self.config, self.num_proxies)
            return True
        else:
            return False

    def client_kill(self):
        logger.info("killing clients ...")
        sites = ProcessInfo.get_sites(self.process_infos, SiteInfo.SiteType.Client)
        hosts = { s.process.host_address for s in sites }
        ps.killall(hosts, "deptran_server", "-9")

    def client_shutdown(self):
        logger.debug("Shutting down clients ...")
        sites = ProcessInfo.get_sites(self.process_infos, SiteInfo.SiteType.Client)
        for site in sites:
            try:
                site.rpc_proxy.sync_client_shutdown()
            except:
                logger.error(traceback.format_exc())

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
    def __init__(self, config, process_infos):
        self.config = config
        self.timeout = config['args'].s_timeout
        self.log_dir = config['args'].log_dir
        taskset = config['args'].s_taskset
        self.recording_path = config['args'].recording_path
        self.process_infos = process_infos
        self.rpc_proxy = dict()
        self.server_kill()

        if (taskset == 1):
            # set task on CPU 1
            self.taskset_func = lambda x: "taskset -c " + str(2 * x + 16)
            logger.info("Setting servers on CPU 1")
        elif (taskset == 2):
            # set task on CPU 0, odd number cores, no overlapping with irq cores
            self.taskset_func = lambda x: "taskset -c " + str(2 * x + 1)
            logger.info("Setting servers on CPU 0, odd number cores")
        elif (taskset == 3):
            # set task on CPU 0, even number cores, overlapping with irq cores
            self.taskset_func = lambda x: "taskset -c " + str(2 * x)
            logger.info("Setting servers on CPU 0, even number cores")
        else:
            self.taskset_func = lambda x: ""
            logger.info("No taskset, auto scheduling")
        self.pre_statistics = dict()
        self.pre_time = time.time()

    def server_kill(self):
        hosts = { pi.host_address for pi in self.process_infos.values() }
        ps_output = ps.ps(hosts, "deptran_server")
        logger.debug("Existing Server or Client Processes:\n{}".format(ps_output))
        ps.killall(hosts, "deptran_server", "-9")
        ps_output = ps.ps(hosts, "deptran_server")
        logger.debug("Existing Server or Client After Kill:\n{}".format(ps_output))

    def setup_heartbeat(self, client_controller):
        logger.debug("in setup_heartbeat")
        cond = multiprocessing.Condition()
        s_init_finish = Value('i', 0)

        do_sample = Value('i', 0)
        do_sample_lock = Lock()

        server_process = multiprocessing.Process(
                target=self.server_heart_beat,
                args=(cond, s_init_finish, do_sample, do_sample_lock))
        server_process.daemon = False
        server_process.start()

        logger.info("Waiting for server init ...")
        cond.acquire()
        while (s_init_finish.value == 0):
            cond.wait()
        if s_init_finish.value == 5:
            logger.error("Waiting for server init ... FAIL")
            raise RuntimeError("server init failed.")
        cond.release()
        logger.info("Waiting for server init ... Done")

        # let all clients start running the benchmark
        client_controller.client_run(do_sample, do_sample_lock)
        cond.acquire()
        s_init_finish.value = 0
        cond.release()
        return server_process

    def shutdown_sites(self, sites):
        for site in sites:
            try:
                site.rpc_proxy.sync_server_shutdown()
            except:
                logger.error(traceback.format_exc())


    def server_heart_beat(self, cond, s_init_finish, do_sample, do_sample_lock):
        logger.debug("in server_heart_beat")
        sites = []
        try:
            sites = ProcessInfo.get_sites(self.process_infos,
                                          SiteInfo.SiteType.Server)
            logger.debug("sites: " + str(sites))
            for site in sites:
                site.connect_rpc(300)
                logger.info("Connected to site %s @ %s", site.name, site.process.host_address)

            for site in sites:

                logger.info("call sync_server_ready on site {}".format(site.id))
                while (site.rpc_proxy.sync_server_ready() != 1):
                    logger.debug("site.rpc_proxy.sync_server_ready returns")
                    time.sleep(1) # waiting for server to initialize
                logger.info("site %s ready", site.name)

            cond.acquire()
            s_init_finish.value = 1
            cond.notify()
            cond.release()

            avg_r_cnt = 0.0
            avg_r_sz = 0.0
            avg_cpu_util = 0.0
            sample_result = []
            while (not g_exit):
                logger.debug("top server heartbeat loop")
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
                cpu_util = [0.0] * len(sites)
                futures = []

                try:
                    for site in sites:
#                        logger.debug("ping %s", site.name)
                        if do_statistics:
                            futures.append(site.rpc_proxy.async_server_heart_beat_with_data())
                        else:
                            futures.append(site.rpc_proxy.async_server_heart_beat())
                except:
                    logger.fatal("server heart beat failure")
                    break


                i = 0
                while (i < len(futures)):
                    if do_statistics:
                        ret = futures[i].result
                        r_cnt_sum += ret.r_cnt_sum
                        r_cnt_num += ret.r_cnt_num
                        r_sz_sum += ret.r_sz_sum
                        r_sz_num += ret.r_sz_num
                        cpu_util[i] = ret.cpu_util
                        logger.info("CPU {}: {}".format(i, ret.cpu_util))
                        for k, v in ret.statistics.items():
                            if k not in statistics:
                                statistics[k] = ServerResponse(v)
                            else:
                                statistics[k].add_one(v)
                    else:
                        futures[i].wait()
                    i += 1
                if do_statistics:
                    total_result = []
                    interval_result = []
                    cur_time = time.time()
                    interval_time = cur_time - self.pre_time
                    self.pre_time = cur_time
                    for k, v in statistics.items():
                        total_result.append([k, v.get_value(), v.get_times(), v.get_ave()])
                        interval_result.append([k, v.get_value(), v.get_times(), v.get_ave(), interval_time])
                    self.pre_statistics = statistics
                    sample_result = interval_result
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

            for single_record in sample_result:
                logger.info("SERVREC: %s; " + str(single_record[0]) + ": VALUE: " +
                            \
                            str(single_record[1]) + "; TIMES: " + \
                            str(single_record[2]) + "; MEAN: " + \
                            str(single_record[3]) + "; TIME: " + \
                            str(single_record[4]))
            logger.info("CPUINFO: " + str(avg_cpu_util))
            logger.info("AVG_LOG_FLUSH_CNT: " + str(avg_r_cnt))
            logger.info("AVG_LOG_FLUSH_SZ: " + str(avg_r_sz))
            logger.info("BENCHMARK SUCCESS!")
        except:
            logger.error(traceback.format_exc())
            cond.acquire()
            s_init_finish.value = 5
            cond.notify()
            cond.release()

    def gen_process_cmd(self, process, host_process_counts):
        cmd = []
        cmd.append("cd " + deptran_home + "; ")
        cmd.append("mkdir -p " + self.log_dir + "; ")
        if (len(self.recording_path) != 0):
            recording = " -r '" + self.recording_path + "/deptran_server_" + process.name + "' "
            cmd.append("mkdir -p " + self.recording_path + "; ")
        else:
            recording = ""

        s = "nohup " + self.taskset_func(host_process_counts[process.host_address]) + \
               " ./build/deptran_server " + \
               "-b " + \
               "-d " + str(self.config['args'].c_duration) + " "

        for fn in self.config['args'].config_files:
            s += "-f '" + fn + "' "

        s += "-P '" + process.name + "' " + \
             "-p " + str(self.config['args'].rpc_port + process.id) + " " \
             "-t " + str(self.config['args'].s_timeout) + " " \
             "-r '" + self.config['args'].log_dir + "' " + \
             recording + \
             "1>'" + self.log_dir + "/proc-" + process.name + ".log' " + \
             "2>'" + self.log_dir + "/proc-" + process.name + ".err' " + \
             "&"

        host_process_counts[process.host_address] += 1
        cmd.append(s)
        return ' '.join(cmd)

    def start(self):
        # this current starts all the processes
        # todo: separate this into a class that starts and stops deptran
        host_process_counts = { host_address: 0 for host_address in self.config['host'].values() }
        def run_one_server(process, process_name, host_process_counts):
            logger.info("starting %s @ %s", process_name, process.host_address)
            cmd = self.gen_process_cmd(process, host_process_counts)
            logger.debug("running: %s", cmd)
            subprocess.call(['ssh', '-f',process.host_address, cmd])

        logger.debug(self.process_infos)

        t_list = []
        for process_name, process in self.process_infos.items():
            time.sleep(0.1) # avoid errors such as ssh_exchange_identification: Connection closed by remote host
            t = Thread(target=run_one_server,
                       args=(process, process_name, host_process_counts,))
            t.start()
            t_list.append(t)

        for t in t_list:
            t.join()


def create_parser():
    parser = ArgumentParser()

    parser.add_argument('-n', "--name", dest="experiment_name",
                        help="name of experiment",
                        default=None)
    parser.add_argument('-id', dest="experiment_id", default="-1")
    parser.add_argument("-f", "--file", dest="config_files",
            help="read config from FILE, default is sample.yml",
            action='append',
            default=[], metavar="FILE")

    parser.add_argument("-P", "--port", dest="rpc_port", help="port to use",
            default=5555, metavar="PORT")

    parser.add_argument("-t", "--server-timeout", dest="s_timeout",
            help="server heart beat timeout in seconds", default=100,
            action="store", metavar="TIMEOUT", type=int)

    parser.add_argument("-i", "--status-time-interval", dest="c_timeout",
            help="time interval to report benchmark status in seconds",
            default=5, action="store", metavar="TIME", type=int)

    parser.add_argument("-w", "--wait", dest="wait",
                        help="wait after starting processes",
                        default=False, action="store_true")

    parser.add_argument("-d", "--duration", dest="c_duration",
            help="benchmark running duration in seconds", default=60,
            action="store", metavar="TIME", type=int)

    parser.add_argument("-S", "--single-server", dest="c_single_server",
            help="control each client always touch the same server "
                 "0, disabled; 1, each thread will touch a single server; "
                 "2, each process will touch a single server",
            default=0, action="store", metavar="[0|1|2]")

    parser.add_argument("-T", "--taskset-schema", dest="s_taskset",
            help="Choose which core to run each server on. "
                 "0: auto; "
                 "1: CPU 1; "
                 "2: CPU 0, odd cores; "
                 "3: CPU 0, even cores;",
            default=0, action="store", metavar="[0|1|2|3]")

    parser.add_argument("-c", "--client-taskset", dest="c_taskset",
            help="taskset client processes round robin", default=False,
            action="store_true")

    parser.add_argument("-l", "--log-dir", dest="log_dir",
            help="Log file directory", default=g_log_dir,
            metavar="LOG_DIR")

    parser.add_argument("-r", "--recording-path", dest="recording_path",
            help="Recording path", default="", metavar="RECORDING_PATH")

    parser.add_argument("-x", "--interest-txn", dest="interest_txn",
            help="interest txn", default=g_interest_txn,
            metavar="INTEREST_TXN")

    parser.add_argument("-H", "--hosts", dest="hosts_path",
            help="hosts path", default="./config/hosts-local",
            metavar="HOSTS_PATH")
    logger.debug(parser)
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
            logger.error("Invalid single server argument.")
            return False

        if not os.path.exists(self.config_path):
            logger.error("Config path incorrect.")
            return False

        if not os.path.exists(self.hosts_path):
            logger.error("Hosts path incorrect.")
            return False

        return True

class SiteInfo:
    class SiteType:
        Client = 1
        Server = 2

    CTRL_PORT_DELTA = 10000
    id = -1

    @staticmethod
    def next_id():
        SiteInfo.id += 1
        return SiteInfo.id

    def __init__(self, process, site_name, site_type, port):
        self.id = SiteInfo.next_id()
        self.process = process
        self.name = site_name
        if type(site_type) == str:
            if site_type == 'client':
                self.site_type = SiteInfo.SiteType.Client
            else:
                self.site_type = SiteInfo.SiteType.Server
        else:
            self.site_type = site_type

        if site_type == 'client':
            self.port = int(port)
            self.rpc_port = int(port)
        elif port is not None:
            self.port = int(port)
            self.rpc_port = self.port + self.CTRL_PORT_DELTA
        else:
            logger.error("server definition should have a port")
            sys.exit(1)


    def connect_rpc(self, timeout):
        logger.debug("in connect_rpc {}".format(self.id))
        if self.site_type == SiteInfo.SiteType.Client:
            if self.process.client_rpc_proxy is not None:
                logger.info("client control rpc already connected for site %s",
                             self.name)
                self.rpc_proxy = self.process.client_rpc_proxy
                return True
            logger.info("start connect to client ctrl rpc for site %s @ %s:%s",
                     self.name,
                     self.process.host_address,
                     self.process.rpc_port)
            port = self.process.rpc_port
        else:
            logger.info("start connect to server ctrl rpc for site %s @ %s:%s",
                     self.name,
                     self.process.host_address,
                     self.rpc_port)
            port = self.rpc_port

        connect_start = time.time()
        self.rpc_client = Client()
        result = None
        while (result != 0):
            bind_address = "{host}:{port}".format(
                host=self.process.host_address,
                port=port)
            # logger.debug("SiteInfo.connect_rpc: rpc_client bind_addr: " + bind_address)
            result = self.rpc_client.connect(bind_address)
            if time.time() - connect_start > timeout:
                raise RuntimeError("rpc connect time out")
            time.sleep(0.1)

        if self.site_type == SiteInfo.SiteType.Client:
            self.process.client_rpc_proxy = ClientControlProxy(self.rpc_client)
            self.rpc_proxy = self.process.client_rpc_proxy
        else:
            self.rpc_proxy = ServerControlProxy(self.rpc_client)
        return True

class ProcessInfo:
    id = -1

    def __init__(self, name, address, rpc_port):
        self.id = ProcessInfo.next_id()
        self.name = name
        self.host_address = address
        self.rpc_port = rpc_port + self.id
        self.client_rpc_proxy = None
        self.sites = []
        logger.info("process {} {} {}:{}".format(name, self.id, address, self.rpc_port))

    def add_site(self, site_name, site_type, port):
        logger.info("add_site: {}, {}, {}".format(site_name, site_type, port))
        obj = SiteInfo(self, site_name, site_type, port)
        self.sites.append(obj)
        return obj

    def client_sites(self):
        return [ site for site in self.sites if site.site_type ==
                SiteInfo.SiteType.Client ]

    def server_sites(self):
        return [ site for site in self.sites if site.site_type ==
                SiteInfo.SiteType.Server ]

    @staticmethod
    def next_id():
        ProcessInfo.id += 1
        return ProcessInfo.id

    @staticmethod
    def get_sites(process_list, site_type):
        sites = []

        if site_type == SiteInfo.SiteType.Client:
            m = ProcessInfo.client_sites
        else:
            m = ProcessInfo.server_sites

        for process in process_list.values():
            for site in m(process):
                sites.append(site)
        return sites


def get_process_info(config):
    hosts = config['host']
    processes = config['process']
    sites = config['site']

    # deterministically give the processes their id -- base client ctrl port assignment on this
    sorted_processes = OrderedDict(sorted(processes.items(), key=lambda t: t[0]))


    process_infos = {}
    for (_, process_name) in sorted_processes.items():
        if process_name not in process_infos:
            process_infos[process_name] = ProcessInfo(process_name,
                                                      hosts[process_name],
                                                      config['args'].rpc_port)

    for site_type in ['server', 'client']:
        for site in itertools.chain(*sites[site_type]):
            if ':' in site:
                site, port = site.split(':')
            else:
                port = int(config['args'].rpc_port)
            pi = process_infos[processes[site]]
            pi.add_site(site, site_type, port)
    logging.info("process infos: {}".format(process_infos))
    return process_infos

def build_config(options):
    config_files = options.config_files
    config = {'args': options}
    for c in config_files:
        with open(c, 'r') as f:
            yml = yaml.load(f)
            config.update(yml)
    return config


def setup_logging(log_file_path=None):
    root_logger = logging.getLogger('')
    root_logger.setLevel(LOG_LEVEL)

    if log_file_path is not None:
        print("logging to file: %s" % log_file_path)
        fh = logging.FileHandler(log_file_path)
        fh.setLevel(LOG_FILE_LEVEL)
        formatter = logging.Formatter(fmt='%(levelname)s: %(asctime)s: %(message)s')
        fh.setFormatter(formatter)
        root_logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setLevel(LOG_LEVEL)
    formatter = logging.Formatter(fmt='%(levelname)s: %(message)s')
    ch.setFormatter(formatter)
    root_logger.addHandler(ch)
    logger.debug('logger initialized')


def setup_experiment(config):
    log_path = None
    if config['args'].experiment_name is not None:
        log_path = os.path.join(config['args'].log_dir,
                                config['args'].experiment_name + ".log")
    setup_logging(log_path)

def main():
    ret = 0
    server_controller = None
    client_controller = None
    config = None
    try:
        config = build_config(create_parser().parse_args())
        setup_experiment(config)

        process_infos = get_process_info(config)
        server_controller = ServerController(config, process_infos)
        logger.debug("before server_controller.start")
        server_controller.start()
        logger.debug("after server_controller.start")

        client_controller = ClientController(config, process_infos)

        if config['args'].wait:
            raw_input("press <enter> to start")

        process = server_controller.setup_heartbeat(client_controller)
        if process is not None:
            process.join()

    except Exception:
        logging.error(traceback.format_exc())
        ret = 1
    finally:
        logging.info("shutting down...")
        if server_controller is not None:
            try:
                #comment the following line when doing profiling
                server_controller.server_kill()
                pass
            except:
                logging.error(traceback.format_exc())
        if ret != 0:
            sys.exit(ret)

if __name__ == "__main__":
    main()
