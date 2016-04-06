#!/usr/bin/env python3
from subprocess import call
from time import time
import argparse

run_app_     = "build/deptran_server"
config_path_ = "config/"

modes_ = [
            "none",
            "tpl_ww",
            "occ",
            "tpl_ww_paxos",
            "occ_paxos",
            "tapir",
            "rcc",
            "brq"
        ]
sites_ =       ["1c1s1p",
                "2c2s1p",
                "64c8s1p",
                "3c3s3r1p",
                "64c8s3r1p"]
benchmarks_ =  ["rw",
                "tpccd"]

def run(m, s, b):
    pm = config_path_ + m + ".yml"
    ps = config_path_ + s + ".yml"
    pb = config_path_ + b + ".yml"

    output_path = m + '-' + s + '-' + b + ".res"
    t1 = time();
    res = "INIT"
    try:
        f = open(output_path, "w")
        r = call([run_app_, "-f", pm, "-f", ps, "-f", pb, "-P", "localhost", "-d", "10"],
                 stdout=f, stderr=f, timeout=5*60)
        res = "OK" if r == 0 else "Failed"
    except:
        res = "Timeout"
    t2 = time();
    print("%-15s \t%-10s\t %s\t %-6s \t %.2fs" % (m, s, b, res, t2-t1))
    pass

def main():
    global modes_
    global sites_
    global benchmarks_
    parser = argparse.ArgumentParser();
    parser.add_argument('-m', '--mode', help='running modes', default=modes_,
                        nargs='+', dest='modes')
    parser.add_argument('-s', '--site', help='sites', default=sites_,
                        nargs='+', dest='sites')
    args = parser.parse_args()
    modes_ = args.modes
    sites_ = args.sites
    for m in modes_:
        for b in benchmarks_:
            for s in sites_:
                run(m, s, b)
    pass

if __name__ == "__main__":
    main()
