#!/usr/bin/env python3

import os
import sys
import time
import logging
import traceback
import subprocess
from threading import Thread
from argparse import ArgumentParser

LOG_LEVEL = logging.INFO
LOG_FILE_LEVEL = logging.DEBUG
logger = logging.getLogger('paxos')

#deptran_home = os.path.split(os.path.realpath(__file__))
#deptran_home = deptran_home[0]
deptran_home = os.getcwd()
g_log_dir = deptran_home + "/log"


def create_parser():
    parser = ArgumentParser()

    parser.add_argument("-N", "--name", dest="experiment_name",
                        help="python log file name", default="paxos-microbench")

    parser.add_argument("-f", "--file", dest="config_files",
                        help="read config from FILE",
                        action='append',
                        default=[], metavar="FILE")

    parser.add_argument("-t", "--server-timeout", dest="s_timeout",
                        help="server heart beat timeout in seconds", default=10,
                        action="store", metavar="TIMEOUT", type=int)

    parser.add_argument("-d", "--duration", dest="c_duration",
                        help="benchmark running duration in seconds", default=60,
                        action="store", metavar="TIME", type=int)

    parser.add_argument("-l", "--log-dir", dest="log_dir",
                        help="log file directory", default=g_log_dir,
                        metavar="LOG_DIR")

    parser.add_argument("-P", "--process-name", dest="process_name",
                        help="all names for each process, MUST be the same as config files",
                        default=[], action='append')

    parser.add_argument("-T", "--total-req", dest="tot_num",
                        help="tot submission numbers for paxos microbench (do not matter if just using submission interfaces)",
                        action="store", default="100", type=int)

    parser.add_argument("-n", "--concurrent-req", dest="concurrent",
                        help="how many outstanding requests for paxos microbench (do not matter if just using submission interfaces)",
                        action="store", default="32", type=int)

    logging.debug(parser)
    return parser


def gen_process_cmd(config, i):
    cmd = []
    if (i >= len(config.process_name)):
        return cmd
    cmd.append("cd " + deptran_home + "; ")
    cmd.append("export LD_LIBRARY_PATH=$(pwd)/build; ")
    # cmd.append("mkdir -p " + config.log_dir + "; ")

    s = "./build/microbench " + \
        "-b " + \
        "-d " + str(config.c_duration) + " "

    for fn in config.config_files:
        s += "-f '" + fn + "' "

    s += "-P " + config.process_name[i] + " " + \
        "-t " + str(config.s_timeout) + " " \
        "-T " + str(config.tot_num) + " " \
        "-n " + str(config.concurrent) + " " \
        "1>'" + config.log_dir + "/proc-" + config.process_name[i] + ".log' " + \
        "2>'" + config.log_dir + "/proc-" + config.process_name[i] + ".err' " + \
        "&"

    cmd.append(s)
    return ' '.join(cmd)


def setup_logging(log_file_path=None):
    root_logger = logging.getLogger('')
    root_logger.setLevel(LOG_LEVEL)
    logger.setLevel(LOG_FILE_LEVEL)

    if log_file_path is not None:
        print("logging to file: %s" % log_file_path)
        fh = logging.FileHandler(log_file_path)
        fh.setLevel(LOG_FILE_LEVEL)
        formatter = logging.Formatter(
            fmt='%(levelname)s: %(asctime)s: %(message)s')
        fh.setFormatter(formatter)
        root_logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setLevel(LOG_LEVEL)
    formatter = logging.Formatter(fmt='%(levelname)s: %(message)s')
    ch.setFormatter(formatter)
    root_logger.addHandler(ch)
    logger.debug('logger initialized')


def main():
    ret = 0
    config = None
    try:
        config = create_parser().parse_args()
        log_path = os.path.join(config.log_dir,
                                config.experiment_name + ".log")
        setup_logging(log_path)

        def run_one_server(address, config, i):
            logger.info("starting %s @ %s", config.process_name[i], address)
            cmd = gen_process_cmd(config, i)
            logger.debug("running: %s", cmd)
            subprocess.call(['ssh', '-f', address, cmd])

        t_list = []
        num_list = list(range(len(config.process_name)))
        num_list.reverse()
        for i in num_list:
            t = Thread(target=run_one_server,
                       args=('127.0.0.1', config, i))
            t.start()
            t_list.append(t)

        for t in t_list:
            t.join()

    except Exception:
        logging.error(traceback.format_exc())
        ret = 1
    finally:
        if ret != 0:
            sys.exit(ret)


if __name__ == "__main__":
    main()
