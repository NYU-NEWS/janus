#!/usr/bin/env python
import os
import os.path
import sys
import itertools
import logging
import traceback
import shutil

prefixes = [ "single_dc", "multi_dc" ]
benchmarks = [ "rw", "rw_fixed", "tpca", "tpca_fixed", "tpcc" ]
zipfs = [ "0.0", "0.25", "0.5", "0.75", "1.0" ]

cwd = os.getcwd()


janus_home = None
if os.environ.get('JANUS_HOME') is None:
    raise RuntimeError("please set JANUS_HOME environment variable")
else:
    janus_home=os.environ['JANUS_HOME']

csv_extractor=os.path.join(janus_home,"scripts","aws","extract_csv.sh")
make_graphs_cmd=os.path.join(janus_home,"scripts","make_graphs")

work_dir = os.path.abspath("./.work_dir")
output_dir = os.path.abspath("./output")
csv_dir = os.path.join(output_dir, "data")

processed_files = {}


def run(c):
    os.system(c)

def rmdir(d):
    logging.info("remove {}".format(d))
    shutil.rmtree(d)

def mkdirp(d):
    try:
        os.makedirs(d)
    except:
        pass

def gen_filename(config):
    (p, b, z) = config
    if b == 'tpca':
        fn = "{}_{}_zipf_{}.tgz".format(*config)
    else:
        fn = "{}_{}.tgz".format(p, b)
    return fn

def get_csv(f):
    cmd = "{} {}".format(csv_extractor, f)
    logging.info("run: {}".format(cmd))
    os.system(cmd)

    cmd = "{} {} {}".format(csv_extractor, f, csv_dir)
    logging.info("run: {}".format(cmd))
    os.system(cmd)

def make_graphs(config):
    (p, b, z) = config
    if b == 'tpca':
        dest = os.path.join(output_dir, p, b, z)
    else:
        dest = os.path.join(output_dir, p, b)

    mkdirp(dest)

    cmd = "{} '*.csv' {}".format(make_graphs_cmd, dest)
    logging.info("run: {}".format(cmd))
    os.system(cmd)

def process_file(f, config):
    if f in processed_files:
        return
    try:
        logging.info("processing {}".format(f))
        processed_files[f] = True
        mkdirp(work_dir)
        run("cp {} {}".format(f, work_dir))
        os.chdir(work_dir)
        get_csv(f)
        make_graphs(config)
    except Exception:
        traceback.print_exc()
    finally:
        os.chdir(cwd)
        rmdir(work_dir) 

def init():
    logging.basicConfig(level=logging.DEBUG)
    mkdirp(output_dir)
    mkdirp(csv_dir)

def main():
    try:
        for config in itertools.product(prefixes, benchmarks, zipfs):
            fn = gen_filename(config)
            if os.path.exists(fn):
                process_file(fn, config)
    except:
        traceback.print_exc()


if __name__ == "__main__":
    init()
    main()
