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

from pylib.ec2 import instance_by_pub_ip, created_instances, EC2_REGIONS

def Xput(*args, **kwargs):
    res = put(*args, **kwargs)
    if res.failed:
        raise RuntimeError('failed transferring {}'.format(args[0]))
    return res


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
    template = string.Template(open(hosts_allow_fn).read())
    ip_list = ""
    contents = StringIO.StringIO(template.substitute(ip_list=ip_list))
    Xput(contents, '/etc/hosts.allow', use_sudo=True)
    Xput("config/etc/exports", "/etc/exports", use_sudo=True)
    sudo('exportfs -a')
    sudo('service nfs-kernel-server start')
    time.sleep(5)
    sudo('service nfs-kernel-server restart')
    time.sleep(5)
    sudo('service nfs-kernel-server reload')


@task
@roles('servers')
def config_nfs_client(server_ip=None):
    if server_ip is None:
        execute('ec2.load_instances')
        instance = instance_by_pub_ip(env.roledefs['leaders'][0])
        if instance is not None and instance.private_ip_address is not None:
            server_ip = instance.private_ip_address
        else:
            raise RuntimeError("can't find leader instance")
    
    fstab_fn = "config/etc/fstab"
    template = string.Template(open(fstab_fn).read())
    contents = StringIO.StringIO(template.substitute(server_ip=server_ip))
    Xput(contents, "/etc/fstab", use_sudo=True)
    reboot()
    with settings(warn_only=True):
        sudo('mount /mnt')

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
def setup_security_groups():
    if 'security_groups' in env:
        return

    execute('ec2.set_instance_roles')
    regions = EC2_REGIONS.keys()
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
    sec_groups = env.security_groups
    regions = created_instances.keys()
    all_ips = [] 
    for region in regions:
        ec2 = boto3.resource('ec2', region_name=region)
        security_group = ec2.SecurityGroup(sec_groups[region])
        if security_group is not None:
            security_group.load()
            logging.info("authorizing region machines for security group {}", sec_grp_name(region))
            try:
                security_group.authorize_ingress(SourceSecurityGroupName=sec_grp_name(region))
            except:
                traceback.print_exc()

            permissions = {
                'IpProtocol': '-1',
                'FromPort': -1,
                'ToPort': -1,
                'IpRanges': []
            }

            for region2 in regions:
                if True or region2 != region:
                    for instance in created_instances[region2]:
                        cidr = instance.public_ip_address + "/32"
                        logging.info("append {} to security group {}".format(
                            cidr, sec_grp_name(region)))
                        permissions['IpRanges'].append({ 'CidrIp': cidr })
            
            logging.info("adding rules: {}", permissions)
            try:
                security_group.authorize_ingress(IpPermissions=[permissions])
            except:
                traceback.print_exc()
            
        else:
            raise RuntimeError("could not load security group")




