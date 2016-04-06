#! /usr/bin/env python
import traceback
import os
import tempfile
import subprocess
from argparse import ArgumentParser
import logging
from logging import info, debug, error 

import yaml

DEFAULT_MODES = ["tpl_ww:none", "occ:none", "tpl_ww:paxos", "occ:paxos",
                 "tapir:tapir", "brq:brq"]
DEFAULT_CLIENTS = ["1:11"]
DEFAULT_SERVERS = ["3:4"]
DEFAULT_BENCHMARKS = [ "rw_benchmark", "tpccd" ]
DEFAULT_TRIAL_DURATION = 10
DEFAULT_EXECUTABLE = "./run.py"
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
                        default = DEFAULT_CLIENTS)
    parser.add_argument("-s", "--server-count", dest="server_counts",
                        help="client counts; accpets " + \
                        "'<start>:<stop>:<step>' tuples with same semantics as " + \
                        "range builtin function.",
                        action='append',
                        default = DEFAULT_SERVERS)
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
                        default = DEFAULT_BENCHMARKS)
    parser.add_argument("-m", "--modes", dest="modes",
                        help="Concurrency control modes; tuple '<cc-mode>:<ab-mode>'",
                        action='append',
                        default = DEFAULT_MODES)
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
        print "file: {}".format(fn)
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
    result = config_files 
    logger.info("result: %s", result)
    return result


def run_experiment(config_file, name, args, benchmark, mode, num_client):
    cmd = [args.executable]
    cmd.extend(['-n', "'{}'".format(args.experiment_name)])
    for c in config_file:
        cmd.extend(['-f', c]) 
    cmd.extend(['-d', args.duration])

    cmd = [str(c) for c in cmd]

    logger.info("running: %s", ' '.join(cmd))
    res=subprocess.call(cmd)
    if res != 0:
        logger.error("subprocess returned %d", res)
    else:
        logger.info("subprocess success.")
    return res


def archive_results(name):
    pass


def generate_graphs(name):
    pass


def run_experiments(args):
    # todo: break up nested loop
    for server_range in args.server_counts:
        for num_server in get_range(server_range):
            for client_range in args.client_counts:
                for num_client in get_range(client_range):
                    for mode in args.modes:
                        for benchmark in args.benchmarks: 
                            experiment_suffix = gen_experiment_suffix(
                                benchmark,
                                mode,
                                num_client)
                            full_experiment_name = "{}-{}".format(
                                args.experiment_name,
                                experiment_suffix) 
                            logger.info("Experiment: {name}".format(
                                name=full_experiment_name))
                            config_file = generate_config(args,
                                                          full_experiment_name,
                                                          benchmark, mode,
                                                          num_client,
                                                          num_server,
                                                          args.num_replicas)
                            try:
                                run_experiment(config_file, full_experiment_name, args,
                                               benchmark, mode, num_client)
                                generate_graphs(full_experiment_name)
                                archive_results(full_experiment_name)
                            except Exception:
                                logger.info("Experiment %s failed.",
                                            full_experiment_name)
                                traceback.print_exc()
                    

def main():
    logging.basicConfig(format="%(levelname)s : %(message)s")
    logger.setLevel(logging.DEBUG)
    args = parse_commandline()
    try:
        run_experiments(args)
    except Exception:
        traceback.print_exc()


if __name__ == "__main__":
    main()
