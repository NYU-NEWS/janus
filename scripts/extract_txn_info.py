#! /usr/bin/env python3
# extracts txn data from the run.py log

import sys
import os
import yaml

try:
    from StringIO import StringIO ## for Python 2
except ImportError:
    from io import StringIO ## for Python 3

BEGIN_DELIM = "__Data__"
END_DELIM = "__EndData__"

if len(sys.argv) != 3:
    print("usage: %s <input-file> <output-file>")
    sys.exit(1)

fn = sys.argv[1]
output_fn = sys.argv[2]
if not os.path.isfile(fn):
    raise IOError("{} not found".format(fn))

record = False
buf = ""
data = {}

with open(fn, 'r') as f:
    for line in f:
        l = line.strip()
        if record and l == END_DELIM:
            record = False
            io = StringIO(buf)
            print(buf)
            y = yaml.load(io)
            txn_name = y['txn_name']
            data[txn_name] = y
        elif not record and l == BEGIN_DELIM:
            record = True
            buf = ""
        elif record:
            buf += l + "\n"

if len(data.keys()) > 0:
    with open(output_fn, 'w') as f:
        f.write(yaml.dump(data))

sys.exit(0)
