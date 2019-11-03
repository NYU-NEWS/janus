import argparse
import logging
import yaml
import copy
import subprocess
from string import Template

default_gnuplot_exe="gnuplot"
default_gnuplot_cmds="./cmds.gnuplot"

args = None

# filters to get the data
def bench(k,*args):
    return k[3] == args[0]

def txn(k, *args):
    return k[0] == args[0]
############################


def parse_args():
    global args
    parser = argparse.ArgumentParser(description='generate graphs.')
    parser.add_argument('-d', '--data', 
                        dest="data_files",
                        metavar='FILE', 
                        type=str, 
                        nargs='+',
                        help='the data files')
    parser.add_argument('-g', '--graph-config', 
                        metavar='FILE',
                        dest="graphs",
                        type=str, 
                        nargs='+',
                        help='the graph config')
    parser.add_argument('-gexe', '--gnuplot-exe',
                        metavar='FILE',
                        dest="gnuplot_exe",
                        type=str, 
                        default=default_gnuplot_exe,
                        help='gnuplot executable')
    parser.add_argument('-gcmds', '--gnuplot-cmds',
                        metavar='FILE',
                        dest="gnuplot_cmds",
                        type=str,
                        default=default_gnuplot_cmds,
                        help='gnuplot commands')
    args = parser.parse_args()


def classify_data():
    global args
    output = {}
    for fn in args.data_files:
        lines = open(fn,'r').readlines()
        for l in lines:
            l = l.strip()
            if l != '' and l[0] != '#':
                key = tuple(l.split(',')[0:4]) # key is txn,cc,ab,bench
                key = tuple([k.strip() for k in key])
                if key not in output:
                    output[key] = fn
                else:
                    logging.warning("duplicate key found for {} - {}; previous {}".format(key, fn, output[key]))
                break
    return output



def get_data(data_classes, filters):
    output = []
    for key, fn in data_classes.items():
        res = []
        for f in filters:
            #print(f[0], f[1:], key[3], f[0](key,*f[1:]))
            res.append( f[0](key,*f[1:]) )
        if all(res):
            output.append(fn)
    return sorted(output)


def run_gnuplot(settings, data_files):
    global args
    
    if (data_files is None) or (len(data_files)==0):
        return

    cmd = [args.gnuplot_exe]
    for k,v in settings.items():
        cmd.append('-e')
        cmd.append("{k}='{v}';".format(k=k, v=v))

    cmd.append('-e')
    cmd.append("input_files='{}'".format(' '.join(data_files)))

    cmd.append(args.gnuplot_cmds)
    logging.info("running: {}".format(' '.join(['"{}"'.format(c) for c in cmd])))
    res = subprocess.call(cmd)

    if res != 0:
        logging.error("Error {} running gnuplot!".format(res))


def generate_graph(config, txn_types, data_classes):
    if type(config['bench']) is not list:
        config['bench'] = [config['bench']]
    logging.info(config)
    if 'output' not in config:
        config['output'] = [ 'png' ]
    for output_type in ['ps']:
        if output_type == 'ps':
            extentsion = '.eps'
            graph_size = '4,3'
        else:
            extentsion = '.png'
            graph_size = '1024,768'

        for txn_name in txn_types:
            for b in config['bench']:
                data_files = get_data(data_classes, [ [bench, b], [txn, txn_name] ])
                logging.debug("generate for {} {}: {}".format(b, txn_name, data_files))
                gnuplot_settings = copy.copy(config['graph'])
                gnuplot_settings['output_type'] = output_type
                if 'graph_size' not in gnuplot_settings:
                    gnuplot_settings['graph_size'] = graph_size 
                gnuplot_settings['output_file'] = \
                    Template(gnuplot_settings['output_file']).substitute(txn=txn_name, bench=b) + \
                    extentsion
                run_gnuplot(gnuplot_settings, data_files)


def main():
    logging.basicConfig(level=logging.DEBUG)
    global args
    parse_args()
    data_classes = classify_data()
    logging.debug("data: {}".format(data_classes))
    txn_types = set([k[0] for k in data_classes.keys()])
    logging.info("Transaction Types: {}".format(txn_types))
    for fn in args.graphs:
        with open(fn, 'r') as f:
            config = yaml.load(f)
            generate_graph(config, txn_types, data_classes)


if __name__ == "__main__":
    main()
