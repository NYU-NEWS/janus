#! /usr/bin/env python3
import sys
import copy
import traceback
import os
import os.path
import tempfile
import subprocess
import itertools
import shutil
import glob
import signal

from argparse import ArgumentParser
from logging import info, debug, error
from pylib.placement_strategy import ClientPlacement, BalancedPlacementStrategy, LeaderPlacementStrategy

import logging
import yaml

if sys.version_info < (3, 0):
    sys.stdout.write("Sorry, requires Python 3.x, not Python 2.x\n")
    sys.exit(1)

def signal_handler(sig, frame):
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

archive_dir = "./archive/"

DEFAULT_MODES = ["tpl_ww:multi_paxos",
                 "occ:multi_paxos",
                 "tapir:tapir",
                 "brq:brq"]

DEFAULT_CLIENTS = ["1:2"]
DEFAULT_SERVERS = ["1:2"]
DEFAULT_BENCHMARKS = ["rw", "tpccd"]
DEFAULT_TRIAL_DURATION = 30
DEFAULT_EXECUTABLE = "./run.py"

APPEND_DEFAULTS = {
    'client_counts': DEFAULT_CLIENTS,
    'server_counts': DEFAULT_SERVERS,
    'benchmarks': DEFAULT_BENCHMARKS,
    'modes': DEFAULT_MODES,
}
TMP_DIR = './tmp'

logger = logging.getLogger('')


def create_parser():
    parser = ArgumentParser()
    parser.add_argument(dest="experiment_name",
                        help="name of experiment")
    parser.add_argument('-e', "--executable", dest="executable",
                        help="the executable to run",
                        default=DEFAULT_EXECUTABLE)
    parser.add_argument("-c", "--client-count", dest="client_counts",
                        help="client counts; accpets " + \
                             "'<start>:<stop>:<step>' tuples with same semantics as " + \
                             "range builtin function.",
                        action='append',
                        default=[])
    parser.add_argument("-cl", "--client-load", dest="client_loads",
                        help="load generation for open clients",
                        nargs="+", type=int, default=[-1])
    parser.add_argument("-st", "--server-timeout", dest="s_timeout", default="30")
    parser.add_argument("-s", "--server-count", dest="server_counts",
                        help="client counts; accpets " + \
                             "'<start>:<stop>:<step>' tuples with same semantics as " + \
                             "range builtin function.",
                        action='append',
                        default=[])
    parser.add_argument("-z", "--zipf", dest="zipf", default=[None],
                        help="zipf values",
                        nargs="+", type=str)
    parser.add_argument("-d", "--duration", dest="duration",
                        help="trial duration",
                        type=int,
                        default=DEFAULT_TRIAL_DURATION)
    parser.add_argument("-r", "--replicas", dest="num_replicas",
                        help="number of replicas",
                        type=int,
                        default=1)
    parser.add_argument('-b', "--benchmarks", dest="benchmarks",
                        help="the benchmarks to run",
                        action='append',
                        default=[])
    parser.add_argument("-m", "--modes", dest="modes",
                        help="Concurrency control modes; tuple '<cc-mode>:<ab-mode>'",
                        action='append',
                        default=[])
    parser.add_argument("-hh", "--hosts", dest="hosts_file",
                        help="path to file containing hosts yml config",
                        default='config/hosts.yml',
                        required=True)
    parser.add_argument("-cc", "--config", dest="other_config",
                        action='append',
                        default=[],
                        help="path to yml config (not containing processes)")
    parser.add_argument("-g", "--generate-graphs", dest="generate_graph",
                        action='store_true', default=False, help="generate graphs")
    parser.add_argument("-cp", "--client-placement", dest="client_placement",
                        choices=[ClientPlacement.BALANCED, ClientPlacement.WITH_LEADER],
                        default=ClientPlacement.BALANCED, help="client placement strategy (with leader for multipaxos)")
    parser.add_argument("-u", "--cpu-count", dest="cpu_count", type=int,
                        default=1, help="number of cores on the servers")
    parser.add_argument("-dc", "--data-centers", dest="data_centers", nargs="+", type=str,
                        default=[], help="data center names (for multi-dc setup)")
    parser.add_argument("--allow-client-overlap", dest="allow_client_overlap",
                        action='store_true', default=False,
                        help="allow clients and server to be mapped to same machine (for testing locally)")
    return parser


def parse_commandline():
    args = create_parser().parse_args()
    for k, v in args.__dict__.items():
        if k in APPEND_DEFAULTS and v == []:
            args.__dict__[k] = APPEND_DEFAULTS[k]
    return args


def gen_experiment_suffix(b, m, c, s, z, cl):
    m = m.replace(':', '-')
    if z is not None:
        return "{}_{}_{}_{}_{}_{}".format(b, m, c, s, z, cl)
    else:
        return "{}_{}_{}_{}_{}".format(b, m, c, s, cl)


def get_range(r):
    a = []
    parts = r.split(':')
    for p in parts:
        a.append(int(p))
    if len(parts) == 1:
        return range(a[0], a[0] + 1)
    else:
        return range(*a)


def gen_process_and_site(args, experiment_name, num_c, num_s, num_replicas, hosts_config, mode):
    hosts = hosts_config['host']

    layout_strategies = {
        ClientPlacement.BALANCED: BalancedPlacementStrategy(),
        ClientPlacement.WITH_LEADER: LeaderPlacementStrategy(),
    }

    if False and mode.find('multi_paxos') >= 0:
        strategy = layout_strategies[ClientPlacement.WITH_LEADER]
    else:
        strategy = layout_strategies[ClientPlacement.BALANCED]

    layout = strategy.generate_layout(args, num_c, num_s, num_replicas, hosts_config)

    if not os.path.isdir(TMP_DIR):
        os.makedirs(TMP_DIR)

    site_process_file = tempfile.NamedTemporaryFile(
        mode='w',
        prefix='janus-proc-{}'.format(experiment_name),
        suffix='.yml',
        dir=TMP_DIR,
        delete=False)

    contents = yaml.dump(layout, default_flow_style=False)

    result = None
    with site_process_file:
        site_process_file.write(contents)
        result = site_process_file.name

    return result


def load_config(fn):
    with open(fn, 'r') as f:
        contents = yaml.load(f)
        return contents


def modify_dynamic_params(args, benchmark, mode, abmode, zipf):
    output_configs = []
    configs = args.other_config

    for config in configs:
        modified = False

        output_config = config
        config = load_config(config)

        if 'bench' in config:
            if 'workload' in config['bench']:
                config['bench']['workload'] = benchmark
                modified = True
            if 'dist' in config['bench'] and zipf is not None:
                try:
                    zipf_value = float(zipf)
                    config['bench']['coefficient'] = zipf_value
                    config['bench']['dist'] = 'zipf'
                except ValueError:
                    config['bench']['dist'] = str(zipf)
                modified = True
        if 'mode' in config and 'cc' in config['mode']:
            config['mode']['cc'] = mode
            modified = True
        if 'mode' in config and 'ab' in config['mode']:
            config['mode']['ab'] = abmode
            modified = True

        if modified:
            f = tempfile.NamedTemporaryFile(
                mode='w',
                prefix='janus-other-{}'.format(args.experiment_name),
                suffix='.yml',
                dir=TMP_DIR,
                delete=False)
            output_config = f.name
            logger.debug("generated config: %s", output_config)
            contents = yaml.dump(config, default_flow_style=False)
            with f:
                f.write(contents)

        output_configs.append(output_config)
    return output_configs


def aggregate_configs(*args):
    logging.debug("aggregate configs: {}".format(args))
    config = {}
    for fn in args:
        config.update(load_config(fn))
    return config


def generate_config(args, experiment_name, benchmark, mode, zipf, client_load, num_client,
                    num_server, num_replicas):
    logger.debug("generate_config: {}, {}, {}, {}, {}".format(
        experiment_name, benchmark, mode, num_client, zipf))
    hosts_config = load_config(args.hosts_file)
    proc_and_site_config = gen_process_and_site(args, experiment_name,
                                                num_client, num_server,
                                                num_replicas, hosts_config, mode)

    logger.debug("site and process config: %s", proc_and_site_config)
    cc_mode, ab_mode = mode.split(':')
    config_files = modify_dynamic_params(args, benchmark, cc_mode, ab_mode,
                                         zipf)
    config_files.insert(0, args.hosts_file)
    config_files.append(proc_and_site_config)
    logger.info(config_files)
    result = aggregate_configs(*config_files)

    if result['client']['type'] == 'open':
        if client_load == -1:
            logger.fatal("must set client load param for open clients")
            sys.exit(1)
        else:
            result['client']['rate'] = client_load

    with tempfile.NamedTemporaryFile(
            mode='w',
            prefix='janus-final-{}'.format(args.experiment_name),
            suffix='.yml',
            dir=TMP_DIR,
            delete=False) as f:
        f.write(yaml.dump(result))
        result = f.name
    logger.info("result: %s", result)
    return result


exp_id = 0


def run_experiment(config_file, name, args, benchmark, mode, num_client):
    global exp_id
    exp_id += 1
    cmd = [args.executable]
    cmd.extend(["-id", exp_id])
    cmd.extend(["-n", "{}".format(name)])
    cmd.extend(["-t", str(args.s_timeout)])
    cmd.extend(["-f", config_file])
    cmd.extend(["-d", args.duration])
    cmd = [str(c) for c in cmd]

    logger.info("running: %s", " ".join(cmd))
    res = subprocess.call(cmd)
    if res != 0:
        logger.error("subprocess returned %d", res)
    else:
        logger.info("subprocess success.")
    return res


def save_git_revision():
    rev_file = "/home/ubuntu/janus/build/revision.txt"
    if os.path.isfile(rev_file):
        cmd = "cat {}".format(rev_file)
        rev = subprocess.getoutput(cmd)
        return rev

    log_dir = "./log/"
    rev = None
    fn = "{}/revision.txt".format(log_dir)
    cmd = 'git rev-parse HEAD'
    with open(fn, 'w') as f:
        logger.info('running: {}'.format(cmd))
        rev = subprocess.getoutput(cmd)
        f.write(rev)
    return rev


def archive_results(name):
    log_dir = "./log/"
    scripts_dir = "./scripts/"
    global archive_dir
    log_file = os.path.join(log_dir, name + ".log")
    data_file = os.path.join(log_dir, name + ".yml")
    archive_file = os.path.join(archive_dir, name + ".tgz")

    try:
        logger.info("copy {} to {}".format(data_file, archive_dir))
        shutil.copy2(data_file, archive_dir)
    except IOError:
        traceback.print_exc()

    archive_cmd = os.path.join(scripts_dir, "archive.sh")
    cmd = [archive_cmd, name]
    logger.info("running: {}".format(" ".join(cmd)))
    res = subprocess.call(cmd)
    if res != 0:
        logger.info("Error {} while archiving.".format(res))


def scrape_data(name):
    log_dir = "./log/"
    scripts_dir = "./scripts/"
    log_file = os.path.join(log_dir, name + ".log")
    output_data_file = os.path.join(log_dir, name + ".yml")

    cmd = [os.path.join(scripts_dir, "extract_txn_info.py"),
           log_file,
           output_data_file]

    logger.info("executing {}".format(' '.join(cmd)))
    res = subprocess.call([os.path.join(scripts_dir, "extract_txn_info.py"),
                           log_file, output_data_file])
    if res != 0:
        logger.error("Error scraping data!!")


def generate_graphs(args):
    if args.generate_graph:
        restore_dir = os.getcwd()
        try:
            archive_dir = "./archive/"
            os.chdir(archive_dir)
            cmd = ['../scripts/make_graphs', "*.csv", ".", ".."]
            res = subprocess.call(cmd)
            if res != 0:
                logger.error('Error generating graphs!!!')
        finally:
            os.chdir(restore_dir)


def aggregate_results(name):
    restore_dir = os.getcwd()
    try:
        archive_dir = "./archive/"
        cc = os.path.join(os.getcwd(), 'scripts/aggregate_run_output.py')
        cmd = [cc, '-p', name, '-r', save_git_revision()]
        os.chdir(archive_dir)
        cmd += glob.glob("{}*yml".format(name))
        res = subprocess.call(cmd)
        if res != 0:
            logger.error("Error aggregating data!!")
    finally:
        os.chdir(restore_dir)


def run_experiments(args):
    server_counts = itertools.chain.from_iterable([get_range(sr) for sr in args.server_counts])
    client_counts = itertools.chain.from_iterable([get_range(cr) for cr in args.client_counts])

    experiment_params = (server_counts,
                         client_counts,
                         args.modes,
                         args.benchmarks,
                         args.zipf,
                         args.client_loads)

    experiments = itertools.product(*experiment_params)
    for params in experiments:
        (num_server, num_client, mode, benchmark, zipf, client_load) = params
        experiment_suffix = gen_experiment_suffix(
            benchmark,
            mode,
            num_client,
            num_server,
            zipf,
            client_load)
        experiment_name = "{}-{}".format(
            args.experiment_name,
            experiment_suffix)

        archive_file = os.path.join(archive_dir, experiment_name + ".tgz")
        if os.path.exists(archive_file):   
            logger.info("skipping test for %s",experiment_name)
            continue

        logger.info("Experiment: {}".format(params))
        config_file = generate_config(
            args,
            experiment_name,
            benchmark, mode,
            zipf,
            client_load,
            num_client,
            num_server,
            args.num_replicas)
        try:
            save_git_revision()
            result = run_experiment(config_file,
                                    experiment_name,
                                    args,
                                    benchmark,
                                    mode,
                                    num_client)
            if result != 0:
                logger.error("experiment returned {}".format(result))
            scrape_data(experiment_name)
            archive_results(experiment_name)
        except Exception:
            logger.info("Experiment %s failed.",
                        experiment_name)
            traceback.print_exc()

    aggregate_results(args.experiment_name)
    generate_graphs(args)


def print_args(args):
    for k, v in args.__dict__.items():
        logger.debug("%s = %s", k, v)


def main():
    logging.basicConfig(format="%(levelname)s : %(message)s")
    logger.setLevel(logging.DEBUG)
    args = parse_commandline()
    print_args(args)
    try:
        os.setpgrp()
        run_experiments(args)
    except Exception:
        traceback.print_exc()
    finally:
        os.killpg(0, signal.SIGTERM)


if __name__ == "__main__":
    main()
