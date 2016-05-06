#!/usr/bin/env python
import os.path
import sys
import traceback
import yaml
import argparse
from itertools import groupby

ARGS = None
labels = ['txn_name', 'cc', 'ab', 'benchmark', 'duration', 'clients',
          'tps', 'attempts', 'commits', 'start_cnt', 'total_cnt']
lat_labels = None
other_labels = [ 'zipf' ]

def parse_args():
    global ARGS
    parser = argparse.ArgumentParser(description="aggregate yml data")
    parser.add_argument('input_files', nargs='+')
    parser.add_argument('--force', '-f', dest="force", action="store_const",
                        const=True, default=False)
    parser.add_argument('--prefix', '-p', dest="prefix", default="")
    parser.add_argument('--rev', '-r', dest="git_revision", default="")
    ARGS = parser.parse_args(sys.argv[1:])

def read_files():
    results=[]
    for fn in ARGS.input_files:
        try:
            with open(fn, 'r') as f:
                results.append(yaml.load(f)) 
        except:
            traceback.print_exc()
    return results

def extract_data(txn_data, file_data):
    for experiment in file_data:
        for txn in experiment.keys():
            row = []
            etxn = experiment[txn]
            for l in labels:
                row.append(etxn[l])
            latencies = etxn['latency']
            global lat_labels
            lat_labels = sorted(latencies.keys())
            for l in lat_labels:
                row.append(latencies[l])
            for l in other_labels:
                row.append(etxn[l])
            if txn not in txn_data:
                txn_data[txn] = []
            a = txn_data[txn]
            a.append(row)

def row_key_clients(r):
    return r[0:4]+[r[5]]
def row_key(r):
    return r[0:4]

def output_data_file(key, group):
    fn = ARGS.prefix + '_'.join(key) + ".csv"
    if os.path.exists(fn) and not ARGS.force:
        raise RuntimeError("file already exists: {}".format(fn))
    print("generate csv: {}".format(fn))
    with open(fn, 'w') as f:
        f.write("# ")
        f.write(", ".join(labels + lat_labels + other_labels))
        f.write("\n")
        f.write("# git sha: {}\n".format(ARGS.git_revision))
        first = True
        for row in group:
            srow = [str(x) for x in row]
          
            f.write(", ".join(srow)) 
            f.write("\n") 
            
            # not sure why, but first line isn't plotted.
            # write it again
            if first:
                f.write(", ".join(srow)) 
                f.write("\n") 
                first = False
                

def main():
    parse_args()
    file_data = read_files()
    txn_data = {}
    extract_data(txn_data, file_data)
    for txn, rows in txn_data.iteritems():
        rows.sort(key=row_key_clients)
        for k, g in groupby(rows, row_key):
            output_data_file(k, g)

if __name__ == "__main__":
    main()
