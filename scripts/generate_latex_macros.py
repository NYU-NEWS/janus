import os
import sys
import re
from argparse import ArgumentParser
import logging
import yaml

kilo_2_significant = lambda x: "%2.2f" % (x / 1000)
two_significant = lambda x: "%2.2f" % x
latency_90 = lambda x: "%2.2f" % x['90.0']

def data_dir(args):
    return os.path.join(args.paper_dir, "figs/graphs")

def get_yaml(directory, contains_string):
    result = []
    all_files = os.listdir(directory)
    for f in all_files:
        full_path = os.path.join(directory, f)
        if os.path.isfile(full_path) and \
           f.find(contains_string) >= 0 and \
           f.endswith('yml'):
            result.append(full_path)
    return result 

class Environment:
    SINGLE = 'single_dc'
    MULTI = 'multi_dc'

class Tests:
    ZIPF = 'tpca_zipf'
    TPCC = 'tpcc'
    RW = 'rw'

class Protocols:
    OCC = 'occ-multi_paxos'
    TPL = '2pl_ww-multi_paxos'
    JANUS = 'brq-brq'
    TAPIR = 'tapir-tapir'

class DataMapping:
    def __init__(self, name, environment, test, protocol, yml_attribute,
                 post_process=None):
        self.name = name
        self.env = environment
        self.test = test
        self.protocol = protocol
        self.yml_attribute = yml_attribute
        self.post_process = post_process
    
    def get_yml_value(self, f, txn):
        value = None
        with open(f, 'r') as ff:
            data = yaml.load(ff)
            value = data[txn][self.yml_attribute]
            self.all_yml = data[txn]
        return value


    def gen_command(self, value):
        return "\\newcommand{\\" + self.name + "}{" + value + "}\n"

    def result(self, v):
        if self.post_process is not None:
            ret = self.gen_command(str(self.post_process(v)))
        else:
            ret = self.gen_command(str(int(v)))
        return ret

class ZipfDataMapping(DataMapping):
    def __init__(self, name, environment, protocol, yml_attribute, zipf,
                 post_process=None):
        DataMapping.__init__(self, name, environment, Tests.ZIPF, protocol,
                             yml_attribute, post_process)
        self.zipf = zipf

    def compute(self, args):
        txn = "PAYMENT"
        yaml_dir = os.path.join(data_dir(args), self.env, self.test)
        yaml_files = get_yaml(yaml_dir, self.protocol)

        zipf_yaml_file = None
        for f in yaml_files:
            if f.find(self.zipf) >= 0:
                zipf_yaml_file = f
                break
        if zipf_yaml_file is None:
            logging.error("cannot find zipf file for {} {} {}".format(self.env,
                                                                 self.test,
                                                                 self.zipf))
            sys.exit(1)
        logging.debug("zipf data file: {}".format(zipf_yaml_file))
        value = self.get_yml_value(zipf_yaml_file, txn)
        return self.result(value)

class ZipfCommitRateMapping(ZipfDataMapping):
    def __init__(self, name, environment, protocol, zipf):
        DataMapping.__init__(self, name, environment, Tests.ZIPF, protocol,
                             'tps') # tps not used
        self.zipf = zipf
        self.post_process = two_significant
    def compute(self, args):
        ZipfDataMapping.compute(self, args)
        return self.result(float(self.all_yml['commits']) /
                           float(self.all_yml['attempts']))


class PeakMapping(DataMapping):
    def __init__(self, name, environment, test, protocol, yml_attribute,
                 post_process=None):
        DataMapping.__init__(self, name, environment, test, protocol,
                             yml_attribute, post_process)

    def compute(self, args):
        logging.debug("compute {}".format(self.name))
        if self.test == Tests.TPCC:
            txn = 'NEW_ORDER'
        elif self.test == Tests.RW:
            txn = 'WRITE'
        else:
            txn = 'PAYMENT'

        stat = -9999999999.999 
        yaml_dir = os.path.join(data_dir(args), self.env, self.test)
        yaml_files = get_yaml(yaml_dir, self.protocol)
        for f in yaml_files:
            value = float(self.get_yml_value(f, txn))
            logging.debug("{}: {}".format(f, value))
            if value > stat:
                stat = value
        return self.result(stat) 



def get_mappings():
    def add_mapping(m, obj):
        m[obj.name] = obj

    m = {}
    
    add_mapping(m, ZipfDataMapping('LanZipfClientCount', 
                                   Environment.SINGLE, Protocols.JANUS, 'clients', "0.0"))
    add_mapping(m, ZipfDataMapping('WanZipfClientCount', 
                                   Environment.MULTI, Protocols.JANUS, 'clients', "0.0"))
    
    add_mapping(m, ZipfDataMapping('LanZipfZeroZeroTapirThroughputKilo', Environment.SINGLE, 
                                   Protocols.TAPIR, 'tps', "0.0", kilo_2_significant))
    add_mapping(m, ZipfDataMapping('LanZipfZeroZeroJanusThroughputKilo', Environment.SINGLE, 
                                   Protocols.JANUS, 'tps', "0.0", kilo_2_significant))
    add_mapping(m, ZipfDataMapping('LanZipfZeroNineJanusThroughputKilo', Environment.SINGLE, 
                                   Protocols.JANUS, 'tps', "0.9", kilo_2_significant))
    add_mapping(m, ZipfDataMapping('LanZipfZeroNineTapirThroughputKilo', Environment.SINGLE, 
                                   Protocols.TAPIR, 'tps', "0.9", kilo_2_significant))

    add_mapping(m, ZipfCommitRateMapping('WanZipfZeroFiveTapirCommitRate', Environment.SINGLE, 
                                   Protocols.TAPIR, "0.5"))

    add_mapping(m, ZipfDataMapping('LanZipfZeroFiveTapirNineZeroLatencyMs', Environment.SINGLE, 
                                   Protocols.TAPIR, 'all_latency', "0.5", latency_90))
    add_mapping(m, ZipfDataMapping('LanZipfZeroFiveJanusNineZeroLatencyMs', Environment.SINGLE, 
                                   Protocols.JANUS, 'all_latency', "0.5", latency_90))
    add_mapping(m, ZipfDataMapping('LanZipfZeroFiveTplNineZeroLatencyMs', Environment.SINGLE, 
                                   Protocols.TPL, 'all_latency', "0.5", latency_90))
    add_mapping(m, ZipfDataMapping('LanZipfZeroFiveOccNineZeroLatencyMs', Environment.SINGLE, 
                                   Protocols.OCC, 'all_latency', "0.5", latency_90))
    add_mapping(m, ZipfDataMapping('LanZipfZeroNineTapirNineZeroLatencyMs', Environment.SINGLE, 
                                   Protocols.TAPIR, 'all_latency', "0.9", latency_90))
    add_mapping(m, ZipfDataMapping('LanZipfZeroNineJanusNineZeroLatencyMs', Environment.SINGLE, 
                                   Protocols.JANUS, 'all_latency', "0.9", latency_90))

    add_mapping(m, ZipfDataMapping('WanZipfZeroNineJanusThroughputKilo', Environment.MULTI, 
                                   Protocols.JANUS, 'tps', "0.9", kilo_2_significant))
    add_mapping(m, ZipfDataMapping('WanZipfZeroNineTapirThroughputKilo', Environment.MULTI, 
                                   Protocols.TAPIR, 'tps', "0.9", kilo_2_significant))
    add_mapping(m, ZipfDataMapping('WanZipfZeroNineOccThroughput', Environment.MULTI, 
                                   Protocols.OCC, 'tps', "0.9"))
    
    add_mapping(m, PeakMapping('LanTpccOccPeakThroughput', 
                               Environment.SINGLE, Tests.TPCC, Protocols.OCC, 'tps'))
    add_mapping(m, PeakMapping('LanTpccTplPeakThroughput', 
                               Environment.SINGLE, Tests.TPCC, Protocols.TPL, 'tps'))
    add_mapping(m, PeakMapping('LanTpccTapirPeakThroughput', 
                               Environment.SINGLE, Tests.TPCC, Protocols.TAPIR, 'tps'))
    add_mapping(m, PeakMapping('LanTpccJanusPeakThroughputKilo', 
                               Environment.SINGLE, Tests.TPCC, Protocols.JANUS,
                               'tps', kilo_2_significant))
    add_mapping(m, PeakMapping('OurTapirPeakThroughputRWKilo', 
                               Environment.SINGLE, Tests.RW, Protocols.TAPIR,
                               'tps', kilo_2_significant))

    return m

def process_file(args, mappings):
    output_data = []
    macro_regex = r"\\newcommand\{\\(.*?)\}"
    with open(args.macro_file,'r') as macro_file:
        for line in macro_file:
            match = re.match(macro_regex, line)
            if match:
                print("match: ", match.group(0))
                name = match.group(1)
                if name in mappings:
                    output_data.append(mappings[name].compute(args))
                else:
                    output_data.append(line)
            else:
                output_data.append(line)

    with open(args.output_file, 'w') as output_file:
        output_file.write(''.join(output_data))

def create_parser():
    parser = ArgumentParser()

    parser.add_argument('-p', "--paper-dir", dest="paper_dir",
                        default=os.environ['janus_paper'],
                        help="paper directory path")
    parser.add_argument('-o', '--output_file', type=str, default="out.tex")
    parser.add_argument('-f', '--input_file', dest="macro_file", type=str, default="eval_numbers.tex")
    return parser
    
def main():
    logging.basicConfig(level=logging.DEBUG)
    args = create_parser().parse_args()
    mappings = get_mappings()
    process_file(args, mappings)

main()
