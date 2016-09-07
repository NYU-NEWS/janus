import logging
import time
import pickle
import os.path
import sys
import socket
import traceback

import boto3
from fabric.api import env, task, run, local, execute, parallel, runs_once
from fabric.decorators import hosts, roles
from fabric.contrib.files import exists
from pylib.security_group import sec_grp_name, save_sec_grp

EC2_REGIONS = {
    'eu-west-1': {
        'location': 'Ireland',
        'endpoint': 'ec2.eu-west-1.amazonaws.com',
        'ami_image': 'ami-26067455'
    },
    'ap-northeast-2': {
        'location': 'Seoul',
        'endpoint': 'ec2.ap-northeast-2.amazonaws.com',
        'ami_image': 'ami-bdf227d3'
    },
    'us-west-2': {
        'location': 'Oregon',
        'endpoint': 'ec2.us-west-2.amazonaws.com',
        'ami_image': 'ami-34579e54'
    },
    'us-west-1': {
        'location': 'California',
        'endpoint': 'ec2.us-west-1.amazonaws.com',
        'ami_image': 'ami-9c730afc'
    },
    'ap-southeast-1': {},
    'ap-southeast-2': {},
    'ap-northeast-1': {},
    'us-east-1': {},
    'sa-east-1': {},
}

INSTANCE_TYPE = 't2.micro'
SECURITY_GROUPS = ['nyu']

# maps region to instances
created_instances = {}
def get_created_instances():
    return created_instances

@task
@roles('leaders')
@parallel(pool_size=10)
def reboot_all():
    instance_ids = []
    for region, instances in get_created_instances().iteritems():
        ec2 = boto3.client('ec2', region_name=region)
        for instance in instances:
            instance_ids.append(instance.id)
        ec2.reboot_instances(InstanceIds=instance_ids)
        instance_ids = []

@task
def list_regions():
    for region, info in EC2_REGIONS.iteritems():
        print ("region: {}".format(region))
        for k in sorted(info.keys()):
            v = info[k]
            print("\t{k}: {v}".format(k=k, v=v))
    print

@task
@hosts('localhost')
def create(region, num=1, instance_type=INSTANCE_TYPE):
    print("ec2.create: ", region, num, instance_type)
    global created_instances
    if not exists('~/.aws/credentials'):
        raise RuntimeError("can't find aws credentials")
    execute('ec2.load_instances')
    execute('cluster.setup_security_groups')
    num = int(num)
    ec2 = boto3.resource('ec2', region_name=region)
    verify_region_has_image(region)
    
    security_group = [ sec_grp_name(region) ]
    print("image id: ", EC2_REGIONS[region]['ami_image'], type(EC2_REGIONS[region]['ami_image']))
    instances = ec2.create_instances(ImageId=EC2_REGIONS[region]['ami_image'],
                                     MinCount=num,
                                     MaxCount=num, 
                                     SecurityGroups=security_group,
                                     InstanceType=instance_type)
    
    logging.info("created {num} instances in region {region}".format(num=num,
                                                             region=region))

    wait_for_ip_address(instances) 

    for instance in instances:
        logging.info(("\ninstance {id}:\n" + \
                     "\tprivate ip {privip}\n" + \
                     "\tpublic ip {pubip}").format(id=instance.instance_id, 
                                                  privip=instance.private_ip_address,
                                                  pubip=instance.public_ip_address))
        if region not in created_instances:
            created_instances[region] = []
        created_instances[region].append(instance)

    persist_instances()
    env.instances_loaded = False


@task
@hosts('localhost')
def load_instances():
    global created_instances
    if 'instances_loaded' in env and env.instances_loaded:
        return

    fn = instance_data_fn()
    
    if not os.path.exists(fn):
        return

    with open(fn, 'rb') as f:
        unpickler = pickle.Unpickler(f)
        data = unpickler.load()
        created_instances = { region: [ get_instance(region, instance_id) for instance_id in instance_ids ]
                              for region, instance_ids in data.iteritems() }
        logging.info("created: {}".format(created_instances))

    env.instances_loaded = True
                 

@task
@hosts('localhost')
def set_instance_roles():
    if env.roledefs is not None and \
       'all' in env.roledefs.keys() and \
       len(env.roledefs['all'])>0:
        # roles already set
        logging.debug("cached instance roles: {}".format(env.roledefs))
        return

    execute('ec2.load_instances')
    roledefs = { 'all': [], 'leaders': [], 'servers': [] }
    
    def add_server(t, ip):
        roledefs['all'].append(ip)
        roledefs[t].append(ip)

    first = True
    for region, instances in created_instances.iteritems():
        for instance in instances:
            if first:
                add_server('leaders', instance.public_ip_address)
                first = False
            else:
                add_server('servers', instance.public_ip_address)
    env.roledefs = roledefs
    logging.info("roles: {}".format(env.roledefs))


def wait_for_ip_address(instances, timeout=120):
    i = 0
    start_time = time.time()
    done = False 
    while not done:
        done = True
        for instance in instances:
            instance.reload()
            if instance.public_ip_address is None or \
               instance.private_ip_address is None:
                done = False
                break

        if not done:
            d = (1.5)**i
            logging.info("wait for ip info.. sleep for {d}, timeout {t}".format(
                d=d, t=timeout))
            delta = time.time() - start_time
            logging.info('elapsed %2.2f s' % delta)
            if delta > timeout:
                break
            time.sleep(d)
            i=i+1
    return done


def verify_region_has_image(region):
    if (region not in EC2_REGIONS) or (EC2_REGIONS[region]['ami_image'] is None):
        raise RuntimeError("no image defined for region {r}".format(r=region))


def instance_data_fn():
    d = "{cwd}/{data_dir}".format(cwd=env.local_cwd, data_dir=env.data_dir)
    if not os.path.isdir(d):
        local("mkdir -p {d}".format(d=d))
    return "{d}/instance.dat".format(d=d)



@task
@hosts('localhost')
def rm_instances_data():
    fn = instance_data_fn()
    if os.path.exists(fn): 
        os.remove(instance_data_fn())
    create_instances = {}


@task
@hosts('localhost')
def terminate_instances():
    execute('ec2.load_instances')
    
    for region, instances in created_instances.iteritems():
        ec2 = boto3.resource('ec2', region_name=region)
        ids = [instance.id for instance in instances]
        logging.info("terminate {}".format(ids))
        ec2.instances.filter(InstanceIds=ids).terminate()

    execute('ec2.rm_instances_data')
    time.sleep(30) 
    for region in created_instances.iterkeys():
        execute('cluster.delete_security_group', region=region)


STATE_RUNNING = 16
@task
@hosts('localhost')
def wait_for_all_servers(timeout=3600):
    n = 5 
    d = 0
    start = time.time()
    done = False
    while not done:
        done = True
        for region, instances in created_instances.iteritems():
            ec2 = boto3.resource('ec2', region_name=region)
            ids = [instance.id for instance in instances]
            instances = ec2.instances.filter(InstanceIds=ids)
            for instance in instances:
                state = instance.state
                if state['Code'] != STATE_RUNNING:
                    done = False
                else:
                    res = ec2.meta.client.describe_instance_status(InstanceIds=[ instance.id ])
                    logging.info(res)
                    if res is not None:
                        sys_status = res['InstanceStatuses'][0]['SystemStatus']['Status']
                        inst_status = res['InstanceStatuses'][0]['InstanceStatus']['Status']
                        if  sys_status != 'ok' or inst_status != 'ok':
                            logging.info("instance %s not ready", instance.id)
                            done = False
                    else:
                        done = False

        elapsed = time.time() - start
        if elapsed > timeout:
            raise RuntimeError("timeout waiting for servers to become ready.") 
        if not done:
            if d<30:
                d = 1.5 ** n
            msg = "wait servers to become ready..\n" + \
                  "sleep for {d:.2f}, elapsed {e:.2f}, timeout {t}"
            logging.info(msg.format(d=d,e=elapsed,t=timeout))
            time.sleep(d)
            n=n+1
    logging.info("done waiting for servers to become ready.")


def persist_instances():
    fn = instance_data_fn()
    with open(fn, 'wb') as f:
        pickler = pickle.Pickler(f)
        data = { region: [ instance.id for instance in instances ] 
                 for region, instances in created_instances.iteritems() }
        pickler.dump(data)


def get_instance(region, instance_id):
    ec2 = boto3.resource('ec2', region_name=region)
    return ec2.Instance(instance_id)


def instance_by_pub_ip(ip):
    execute('ec2.load_instances')
    print('looking: {}, {}'.format(ip, created_instances))
    for region, instances in created_instances.iteritems():
        logging.info("region {}, ins: {}".format(region, instances))
        for instance in instances:
            print ("instance id: {}; ip {}".format(instance.id,
                                                   instance.public_ip_address))
            print("{} == {}".format(ip, instance.public_ip_address))
            if instance.public_ip_address == ip:
                return instance
    return None


