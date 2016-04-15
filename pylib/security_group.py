import os
import yaml
import random
from fabric.api import env, local

def security_group_fn():
    d = "{cwd}/{data_dir}".format(cwd=env.local_cwd, data_dir=env.data_dir)
    if not os.path.isdir(d):
        local("mkdir -p {d}".format(d=d))
    return "{d}/sec_group.dat".format(d=d)

def sec_grp_name(region):
    sec_group_data = None
    if os.path.exists(security_group_fn()):
        with open(security_group_fn(), 'r') as f:
            sec_group_data = yaml.load(f) 
    
    if sec_group_data is None or region not in sec_group_data:
        ident = ''.join([random.choice('ABCDEF0123456789') for _ in range(8)])
        sg_name = 'sg_janus_{ident}_{region}'.format(ident=ident, region=region)
        save_sec_grp(region, sg_name)
        return sg_name
    else:
        return sec_group_data[region]

def save_sec_grp(region, name):
    sec_group_data = {} 
    if os.path.exists(security_group_fn()):
        with open(security_group_fn(), 'r') as f:
            sec_group_data = yaml.load(f)
    sec_group_data[region] = name
    with open(security_group_fn(), 'w') as f:
        f.write(yaml.dump(sec_group_data))

