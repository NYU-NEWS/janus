#!/usr/bin/env python3
import subprocess
import sys
import logging
import argparse

#default_zipfs = [ '0.0', '0.25', '0.5', '0.75', '1.0' ]
default_zipfs = [ '0.0', '0.1', '0.2', '0.3', '0.4', '0.5', '0.6', '0.7', '0.8', '0.9', '1.0' ]

default_configs = [ 
    'config/concurrent_50.yml',
    'config/tpca_zipf.yml',
    'config/tapir.yml'
]
#default_modes = [ 'brq:brq', 'tapir:tapir', 'troad:troad', '2pl_ww:multi_paxos', 'occ:multi_paxos' ]
default_modes = [ 'brq:brq', '2pl_ww:multi_paxos', 'occ:multi_paxos', 'troad:troad' ]
#default_modes = [ 'brq:brq', 'troad:troad']
#default_modes = [ 'tapir:tapir' ]
default_benchmark = [ 'tpca' ]
default_options = ['-r', '3']

def parse_args():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('name', default='foo')
    parser.add_argument('--zipf', metavar='N', 
            dest='zipf', nargs='+', default=default_zipfs,
            help='The zipf distribution coefficients: (e.g. 0.0 0.25)')
    parser.add_argument('--clients', '-c', metavar='N', nargs='+',
            dest='clients', default=['1'],
            help='number of clients')
    parser.add_argument('--hosts', '-hh', metavar='FILE', 
            dest = 'hosts', default='config/aws_hosts.yml',
            help='hosts config')
    parser.add_argument('-f', '--file', nargs='+',
                        dest='config_files', default=default_configs)
    parser.add_argument('-b', '--bench', nargs='+',
                        dest='bench', default=default_benchmark)
    parser.add_argument('-o', '--options', nargs='+',
                        dest='options', default=default_options)
    parser.add_argument('-m', '--modes', nargs='+',
                        dest='modes', default=default_modes)
    parser.add_argument('-s', '--shards', dest='shards', default='6')
    parser.add_argument('-d', '--duration', dest='duration', default='90')
    parser.add_argument('-cl', '--client-load', dest='client_load', default=None)
    parser.add_argument('-dc', '--data-centers', dest='data_centers', default=None, nargs='+', type=str)
    parser.add_argument('-u', '--cpu', dest='num_cpu', default=1, type=int)
	
    return parser.parse_args()

def run(config, zipf):
    cmd = ['./run_all.py', config.name,
               '-hh', config.hosts, '--allow-client-overlap']
    for f in config.config_files:
        cmd.extend(['-cc', f])
    for m in config.modes:
        cmd.extend(['-m', m])
    for b in config.bench:
        cmd.extend(['-b', b])
    for c in config.clients:
        cmd.extend(['-c', c])
    cmd.extend(['-z', zipf])
    cmd.extend(config.options)
    cmd.extend(['-s', config.shards])
    cmd.extend(['-d', config.duration])
    cmd.extend(['-u', str(config.num_cpu)])
    
    if config.data_centers is not None:
        cmd.extend(['-dc'] + config.data_centers)

    if config.client_load is not None:
        cmd.extend(['-cl', config.client_load])
    
    logging.info('> {}'.format(' '.join(cmd)))
    logging.info(cmd)
    subprocess.call(cmd)

def main():
    logging.basicConfig(level=logging.DEBUG)
    config = parse_args()
    for z in config.zipf:
        run(config, z)

if __name__ == "__main__":
    main()
