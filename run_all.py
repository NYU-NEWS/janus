import traceback
import os
from argparse import ArgumentParser
import logging
from logging import info, debug, error 

import yaml

DEFAULT_MODES = ["none", "tpl_ww", "occ", "tpl_ww_paxos", "occ_paxos",
                 "tapir", "brq"]
DEFAULT_CLIENTS = ["1:11:1"]
DEFAULT_BENCHMARKS = [ "rw_benchmark", "tpccd" ]

logger = logging.getLogger('')

def create_parser():
    parser = ArgumentParser()
    
    parser.add_argument('-n', "--name", dest="experiment_name",
                        help="name of experiment",
                        default=None)
    parser.add_argument("-c", "--client-count", dest="client_counts",
                        help="client counts; accpets " + \
                        "'<start>:<stop>:<step>' tuples with same semantics as " + \
                        "range builtin function.",
                        action='append',
                        default = DEFAULT_CLIENTS)
    parser.add_argument('-b', "--benchmarks", dest="benchmarks",
                        help="the benchmarks to run",
                        action='append',
                        default = DEFAULT_BENCHMARKS)
    parser.add_argument("-m", "--modes", dest="modes",
                        help="Concurrency control modes",
                        action='append',
                        default = DEFAULT_MODES)

    return parser


def parse_commandline():
    args = create_parser().parse_args()
    return args


def gen_experiment_suffix(b, m, c):
    return "{}-{}-{}".format(b, m, c)


def get_range(r):
    a = []
    parts = r.split(':')
    for p in parts:
        a.append(int(p))
    return range(*a)


def run_experiments(args):
    for benchmark in args.benchmarks: 
        for mode in args.modes:
            for client_range in args.client_counts:
                for num_client in get_range(client_range):
                    experiment_suffix = gen_experiment_suffix(
                        benchmark,
                        mode,
                        num_client)
                    logger.info("Experiment: {name}-{suffix}".format(
                        name=args.experiment_name,
                        suffix=experiment_suffix))


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
