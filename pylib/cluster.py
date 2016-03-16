import logging
import string
import StringIO
import time

from fabric.api import env, task, run, sudo, local
from fabric.api import put, execute, cd, runs_once, reboot
from fabric.contrib.files import exists
from fabric.decorators import roles, parallel
from fabric.context_managers import prefix

from pylib.ec2 import instance_by_pub_ip

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

