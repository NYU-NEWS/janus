import logging
import string
import StringIO
import time
import traceback

import boto3

from fabric.api import env, task, run, sudo, local
from fabric.api import put, execute, cd, runs_once, reboot, settings
from fabric.contrib.files import exists
from fabric.decorators import roles, parallel, hosts
from fabric.context_managers import prefix

from pylib.ec2 import instance_by_pub_ip, EC2_REGIONS, get_created_instances

# Static ip address ranges allowed to contact the instances
ALLOWED_IP_RANGES = [ '128.122.140.0/24' ]

def Xput(*args, **kwargs):
    res = put(*args, **kwargs)
    if res.failed:
        raise RuntimeError('failed transferring {}'.format(args[0]))
    return res

@task
@roles('leaders')
def put_janus_config():
    import yaml
    execute('ec2.set_instance_roles')
    config_fn = 'config/aws.yml'
    config = yaml.load(open(config_fn).read())
    config['host'] = {}
    host = config['host']
    created_instances = get_created_instances()
    leader_ip_address = env.roledefs['leaders'][0]
    for region, instances in created_instances.iteritems():
        cnt = 0
        for instance in instances:
            proc_name = "{region}-{cnt}".format(region=region, cnt=cnt)
            if instance.public_ip_address != leader_ip_address:
                host[proc_name] = instance.public_ip_address
                cnt += 1
    config_contents = StringIO.StringIO(yaml.dump(config, default_flow_style=False))
    put(config_contents, "{}/config/aws.yml".format(env.nfs_home))



@task
@runs_once
@roles('leaders')
def config_nfs_server():
    cmds = [
        'apt-get -y install nfs-kernel-server',
        'service nfs-kernel-server stop',
        'mkdir -p /export',
        'chmod 777 /export',
    ]
    for c in cmds:
        sudo(c)
    
    Xput('config/etc/hosts.deny', '/etc/hosts.deny', use_sudo=True)
    hosts_allow_fn = 'config/etc/hosts.allow'
    template_ha = string.Template(open(hosts_allow_fn).read())
    exports_fn = 'config/etc/exports'
    template_e = string.Template(open(exports_fn).read())

    export_options = "(rw,fsid=0,insecure,no_subtree_check,async)"
    ip_options = []
    for ip in env.roledefs['all']:
        ip_options.append("{ip}{opt}".format(ip=ip, opt=export_options))
    contents = StringIO.StringIO(template_e.substitute(host_and_options= \
                                                       ' '.join(ip_options)))
    Xput(contents, "/etc/exports", use_sudo=True)

    ip_list = ' '.join(env.roledefs['all'])
    contents = StringIO.StringIO(template_ha.substitute(ip_list=ip_list))
    logging.info("/etc/hosts.allow :\n{}".format(contents.getvalue()))
    Xput(contents, '/etc/hosts.allow', use_sudo=True)

    sudo('exportfs -a')
    reboot()


@task
@roles('servers', 'leaders')
def config_nfs_client(server_ip=None):
    if server_ip is None:
        execute('ec2.load_instances')
        instance = instance_by_pub_ip(env.roledefs['leaders'][0])
        if instance is not None and instance.public_ip_address is not None:
            server_ip = instance.public_ip_address
        else:
            raise RuntimeError("can't find leader instance or no public ip")

    logging.info("using {} for the nfs server".format(server_ip))
    fstab_fn = "config/etc/fstab"
    template = string.Template(open(fstab_fn).read())
    contents = StringIO.StringIO(template.substitute(server_ip=server_ip))
    Xput(contents, "/etc/fstab", use_sudo=True)
    try:
        sudo('mount /mnt')
    except:
        traceback.print_exc()


def sec_grp_name(region):
    return 'sg_janus_{}'.format(region)


@task
@hosts('localhost')
def delete_security_group(region):
    if region is None:
        return

    client = boto3.client('ec2', region_name=region)
    try:
        client.delete_security_group(GroupName=sec_grp_name(region))
    except:
        traceback.print_exc()


@task
@hosts('localhost')
@runs_once
def setup_security_groups(regions=EC2_REGIONS.keys()):
    if 'security_groups' in env:
        return

    if isinstance(regions, basestring):
        regions = regions.split(":")

    execute('ec2.set_instance_roles')
    roledefs = env.roledefs
     
    sec_groups = {}
    for region in regions:
        client = boto3.client('ec2', region_name=region)
        res = None
        try:
            res = client.create_security_group(
                GroupName=sec_grp_name(region),
                Description='janus security group')
        except:
            traceback.print_exc()
            groups = client.describe_security_groups(GroupNames=[sec_grp_name(region)])
            group_id = groups['SecurityGroups'][0]['GroupId']
            res = {'GroupId': group_id}

        if res is not None and 'GroupId' in res:
            sec_groups[region] = res['GroupId']
        else:
            raise RuntimeError("could not create security group.")
    env.security_groups = sec_groups
    
@task
@hosts('localhost')
def load_security_grp_ips():
    execute('ec2.load_instances')
    execute('cluster.setup_security_groups')
    sec_group_ids = env.security_groups
    created_instances = get_created_instances()
    logging.info("created instances: {}".format(created_instances))
    regions = created_instances.keys()
    logging.info("setup security group for regions: {}".format(regions))
    for region in regions:
        ec2 = boto3.resource('ec2', region_name=region)
        logging.info("adding ips to security group {}".format(sec_grp_name(region)))
        security_group = ec2.SecurityGroup(sec_group_ids[region])
        if security_group is not None:
            security_group.load()

            permissions = {
                'IpProtocol': '-1',
                'FromPort': -1,
                'ToPort': -1,
                'IpRanges': []
            }
            
            for region2 in regions:
                for instance in created_instances[region2]:
                    cidr = instance.public_ip_address + "/32"
                    permissions['IpRanges'].append({ 'CidrIp': cidr })
            
            for cidr in ALLOWED_IP_RANGES:
                permissions['IpRanges'].append({ 'CidrIp': cidr })

            logging.info("add rules to security group {}:\n{}".format(
                sec_grp_name(region), permissions))
            try:
                security_group.authorize_ingress(SourceSecurityGroupName=sec_grp_name(region))
                security_group.authorize_ingress(IpPermissions=[permissions])
            except:
                traceback.print_exc()
            
        else:
            raise RuntimeError("could not load security group")


