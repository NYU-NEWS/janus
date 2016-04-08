#! /usr/bin/env python
import sys
import traceback
import os
import tempfile
import subprocess
import itertools
import shutil
from argparse import ArgumentParser
import logging
from logging import info, debug, error 

import yaml

DEFAULT_MODES = ["tpl_ww:none",
                 "occ:none",
                 "tpl_ww:paxos",
                 "occ:paxos",
                 "tapir:tapir",
                 "brq:brq"]

DEFAULT_CLIENTS = ["1:2"]
DEFAULT_SERVERS = ["1:2"]
DEFAULT_BENCHMARKS = [ "rw_benchmark", "tpccd" ]
DEFAULT_TRIAL_DURATION = 30
DEFAULT_EXECUTABLE = "./run.py"

APPEND_DEFAULTS = {
    'client_counts': DEFAULT_CLIENTS,
    'server_counts': DEFAULT_SERVERS, 
    'benchmarks': DEFAULT_BENCHMARKS,
    'modes': DEFAULT_MODES,
}
TMP_DIR='./tmp'

logger = logging.getLogger('')

def create_parser():
    parser = ArgumentParser()
    parser.add_argument(dest="experiment_name",
                        help="name of experiment")
    parser.add_argument('-e', "--executable", dest="executable",
                        help="the executable to run",
                        default = DEFAULT_EXECUTABLE)
    parser.add_argument("-c", "--client-count", dest="client_counts",
                        help="client counts; accpets " + \
                        "'<start>:<stop>:<step>' tuples with same semantics as " + \
                        "range builtin function.",
                        action='append',
                        default=[])
    parser.add_argument("-s", "--server-count", dest="server_counts",
                        help="client counts; accpets " + \
                        "'<start>:<stop>:<step>' tuples with same semantics as " + \
                        "range builtin function.",
                        action='append',
                        default =[])
    parser.add_argument("-d", "--duration", dest="duration",
                        help="trial duration",
                        type=int,
                        default = DEFAULT_TRIAL_DURATION)
    parser.add_argument("-r", "--replicas", dest="num_replicas",
                        help="number of replicas",
                        type=int,
                        default = 1)
    parser.add_argument('-b', "--benchmarks", dest="benchmarks",
                        help="the benchmarks to run",
                        action='append',
                        default =[])
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
    return parser


def parse_commandline():
    args = create_parser().parse_args()
    for k,v in args.__dict__.iteritems():
        if k in APPEND_DEFAULTS and v == []:
            args.__dict__[k] = APPEND_DEFAULTS[k] 
    return args


def gen_experiment_suffix(b, m, c):
    m = m.replace(':', '_')
    return "{}-{}-{}".format(b, m, c)


def get_range(r):
    a = []
    parts = r.split(':')
    for p in parts:
        a.append(int(p))
    return range(*a)

def gen_process_and_site(experiment_name, num_c, num_s, num_replicas, hosts_config):
    process_map = {}
    clients = [] 
    servers = [] 
    hosts = hosts_config['host']	

    for x in range(num_c):
        clients.append(['c'+str(x)])
    
    x=0
    while x<num_s:
        l = []
        r = 0
        while r<num_replicas:
            l.append('s'+str(x)+':'+str(x+10000))
            r += 1
            x += 1
        servers.append(l)

    # allocate servers 1 to a process;
	# clients assigned round robin to remaining processes
    t = (num_s*num_replicas)+1 
    process_names = []
    if t > len(hosts):
        host_processes = hosts.keys()
        i=0
        while len(process_names) < t:
            process_names.append(host_processes[i%len(host_processes)])
            i += 1
        process_names = sorted(process_names)
    else:
        process_names = sorted(hosts.keys())
    process_names.reverse()

    for s in servers:
        assign_to = process_names.pop()
        s_name = s[0].split(':')[0]
        process_map[s_name] = assign_to

    idx = 0
    for c in clients:
        c_name = c[0]
        assign_to = process_names[idx % len(process_names)]
        idx += 1
        process_map[c_name] = assign_to
    
    site_process_config = {'site': {}}
    site_process_config['site']['client'] = clients 
    site_process_config['site']['server'] = servers 
    site_process_config['process'] = process_map

    if not os.path.isdir(TMP_DIR):
        os.makedirs(TMP_DIR)

    site_process_file = tempfile.NamedTemporaryFile(
        mode='w', 
        prefix='janus-proc-{}'.format(experiment_name),
        suffix='.yml',
        dir=TMP_DIR,
        delete=False)
                                                
    contents = yaml.dump(site_process_config, default_flow_style=False)
    result = None 
    with site_process_file:
        site_process_file.write(contents)
        result = site_process_file.name
    return result 

def load_config(fn):
    with open(fn, 'r') as f:
        contents = yaml.load(f)
        return contents

def modify_benchmark_and_mode(args, benchmark, mode, abmode):
    output_configs = []
    configs = args.other_config
    
    for config in configs:
        modified = False

        output_config = config
        config = load_config(config)

        if 'bench' in config and 'workload' in config['bench']:
            config['bench']['workload'] = benchmark
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
    print(args)
    config = {}
    for fn in args:
        config.update(load_config(fn))
    return config


def generate_config(args, experiment_name, benchmark, mode, num_client,
                    num_server, num_replicas):
    logger.debug("generate_config: {}, {}, {}, {}".format(
        experiment_name, benchmark, mode, num_client))
    hosts_config = load_config(args.hosts_file)
    proc_and_site_config = gen_process_and_site(experiment_name, num_client,
                                                num_server, num_replicas, 
                                                hosts_config)
    logger.debug("site and process config: %s", proc_and_site_config)
    cc_mode, ab_mode = mode.split(':')
    config_files = modify_benchmark_and_mode(args, benchmark, cc_mode, ab_mode) 
    config_files.insert(0, args.hosts_file)
    config_files.append(proc_and_site_config)
    logger.info(config_files)
    result = aggregate_configs(*config_files)
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


def run_experiment(config_file, name, args, benchmark, mode, num_client):
    cmd = [args.executable]
    cmd.extend(["-n", "{}".format(name)])
    cmd.extend(["-f", config_file]) 
    cmd.extend(["-d", args.duration])
    cmd = [str(c) for c in cmd]

    logger.info("running: %s", " ".join(cmd))
    res=subprocess.call(cmd)
    if res != 0:
        logger.error("subprocess returned %d", res)
    else:
        logger.info("subprocess success.")
    return res


def archive_results(name):
    log_dir = "./log/"
    scripts_dir = "./scripts/"
    archive_dir = "./archive/"
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
    res=subprocess.call([os.path.join(scripts_dir, "extract_txn_info.py"),
                         log_file, output_data_file])
    if res!=0:
        logger.error("Error scraping data!!")


def run_experiments(args):
    server_counts = itertools.chain.from_iterable([get_range(sr) for sr in args.server_counts])
    client_counts = itertools.chain.from_iterable([get_range(cr) for cr in args.client_counts])

    experiment_params = (server_counts,
                         client_counts,
                         args.modes,
                         args.benchmarks)

    experiments = itertools.product(*experiment_params)
    for params in experiments:
        (num_server, num_client, mode, benchmark) = params 
        experiment_suffix = gen_experiment_suffix(
            benchmark,
            mode,
            num_client)
        experiment_name = "{}-{}".format(
            args.experiment_name,
            experiment_suffix)

        logger.info("Experiment: {}".format(params))
        config_file = generate_config(
            args,
            experiment_name,
            benchmark, mode,
            num_client,
            num_server,
            args.num_replicas)
        try:
            result = run_experiment(config_file, 
                                    experiment_name, 
                                    args, 
                                    benchmark, 
                                    mode, 
                                    num_client)
            if result == 0:
                scrape_data(experiment_name)

            archive_results(experiment_name)
        except Exception:
            logger.info("Experiment %s failed.",
                        experiment_name)
            traceback.print_exc()
                   

def print_args(args):
    for k,v in args.__dict__.iteritems():
        logger.debug("%s = %s", k, v)


def main():
    logging.basicConfig(format="%(levelname)s : %(message)s")
    logger.setLevel(logging.DEBUG)
    args = parse_commandline()
    print_args(args)
    try:
        run_experiments(args)
    except Exception:
        traceback.print_exc()


if __name__ == "__main__":
    main()
