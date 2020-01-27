import logging
import os
import traceback
import time
import random
from string import Template
from six import string_types
try:
    from StringIO import StringIO ## for Python 2
except ImportError:
    from io import StringIO ## for Python 3


from fabric.api import env, task, run, local, hosts
from fabric.api import execute, cd, runs_once, sudo, parallel
from fabric.contrib.files import exists, append
from fabric.decorators import roles
from fabric.context_managers import prefix
from fabric.context_managers import settings
from fabric.context_managers import hide
from fabric.operations import reboot

# tasks are exported under the module names
from pylib import ec2
from pylib import cluster
from pylib.cluster import Xput

import pylib

# username for ec2 instances
env.user = 'ubuntu'
env.key_filename = 'config/ssh/id_rsa'

settings(hide('warnings', 'running', 'stdout', 'stderr'),warn_only=True)

def run_python(*args, **kwargs):
    activate_virtual_env = \
        "source {venv}/bin/activate".format(
            venv=env.py_virtual_env)
    with prefix(activate_virtual_env):
        run(*args, **kwargs)


def init():
    logging.basicConfig(level=logging.INFO)
    environment()


def environment():
    if '__envset__' not in env:
        env.__envset__ = True
        
        # standard fabric variables
        env.connection_attempts = 10
        env.timeout = 30 
        env.use_ssh_config = True
        
        # custom variables
        env.local_cwd = os.path.dirname(os.path.realpath(__file__))
        env.setdefault('remote_home', '/mnt/janus')
        env.setdefault('nfs_home', '/export/janus')
        env.setdefault('git_repo', 'git@github.com:shuaimu/janus.git')
        env.setdefault('git_revision', 'tmp3')
        env.setdefault('py_virtual_env',
                       '{home}/py_venv'.format(home=env.nfs_home))
        env.setdefault('data_dir', '.ec2-data')


@task
@hosts('localhost')
def deploy_continue():
    execute('ec2.set_instance_roles')
    execute('cluster.config_nfs_server')
    execute('cluster.config_nfs_client')
    execute('cluster.config_ssh')
    execute('retrieve_code')
    execute('create_work_dirs')
    teardown_instances = False 
    attempts = 0
    done = False
    while attempts < 3 and done == False:
        try:
            execute('install_apt_packages')
            execute('install_leader_apt_packages')
#            execute('config_ntp_leaders')
#            execute('config_ntp_clients')
            execute('build', args="-t")
            execute('cluster.put_janus_config')
            execute('cluster.put_limits_config')
            execute('cluster.mount_nfs')
            execute('ec2.reboot_all')
            done = True
        except:
            attempts = attempts + 1


@task
@runs_once
@hosts('localhost')
def deploy_all(regions='us-west-2', servers_per_region=[3], instance_type='t2.small'):
    """
    keyword arguments:
        regions (string) - colon separated list of regions to deploy to; 
            default 'us-west-2'
        servers_per_region (string) - colon separated list indicating 
            the number of servers in each region; 
            default 3 
        instance_type - the ec2 instance type; default 't2.small'
    example:
         fab deploy_all:regions=us-west-2:eu-west-1,servers_per_region=3:2
    """
    if isinstance(regions, string_types):
        regions = regions.split(':')
    if isinstance(servers_per_region, string_types):
        servers_per_region = [ int(i) for i in servers_per_region.split(':') ]

    teardown_instances = True 
    assert(len(servers_per_region) == len(regions))

    start = time.time()
    try:
        logging.info('deploy to regions: {}'.format(','.join(regions)))
        for region in regions:
            execute('cluster.delete_security_group', region=region)
        execute('cluster.setup_security_groups', regions=regions)
        
        region_index = 0
        for region in regions:
            logging.debug('create in region: {}'.format(region))
            execute('ec2.create', 
                    region=region,
                    num=servers_per_region[region_index], 
                    instance_type=instance_type)
            region_index += 1

        execute('cluster.load_security_grp_ips')
        execute('ec2.set_instance_roles')
        ec2.wait_for_all_servers()
        execute('cluster.config_nfs_server')
        execute('cluster.config_nfs_client')
        execute('cluster.config_ssh')
        execute('retrieve_code')
        execute('create_work_dirs')
        teardown_instances = False 
        attempts = 0
        done = False
        while attempts < 3 and done == False:
            try:
                execute('install_leader_apt_packages')
#                execute('config_ntp_leaders')
#                execute('config_ntp_clients')
                execute('build', args="-t")
                execute('cluster.put_janus_config')
                execute('cluster.put_limits_config')
                execute('cluster.mount_nfs')
                execute('ec2.reboot_all')
                done = True
            except:
                attempts = attempts + 1

    except Exception as e:
        traceback.print_exc()
        if teardown_instances:
            logging.info("Terminating ec2 instances...")
            ec2.terminate_instances()
    finally:
        print("{:.2f} seconds elapsed".format(time.time() - start))


@task
@runs_once
@roles('leaders')
def create_virtual_env():
    venv = env.py_virtual_env
    append('/home/ubuntu/.bash_profile',
           'source {venv}/bin/activate'.format(venv=venv))
    #if exists(env.py_virtual_env):
    #    return
    
    with cd(env.nfs_home):
        execute('retrieve_code')
        run('pyenv local 3.8')
        run('virtualenv {venv}'.format(venv=venv))
        run('{venv}/bin/pip install -r requirements.txt'.format(venv=venv))


@task
@runs_once
@roles('leaders')
def create_work_dirs():
    dirs = ['tmp/', 'log/', 'archive/']
    for d in dirs:
        dir_path = os.path.join(env.nfs_home, d)
        run("mkdir -p {}".format(dir_path))
 

@task
@runs_once
@roles('leaders')
def install_leader_apt_packages():
    with settings(hide('warnings', 'running', 'stdout', 'stderr'),warn_only=True):
        sudo('apt-get -y update -qq')
        sudo('apt-get -y dist-upgrade')
        sudo('apt-get -y install gnuplot')
 
@task
@roles('all')
@parallel
def install_apt_packages():
    with settings(hide('warnings', 'running', 'stdout', 'stderr'),warn_only=True):
        sudo('apt-get -y -qq update')
        sudo('apt-get -y -qq dist-upgrade')
        sudo('apt-get -y -qq install pkg-config libgoogle-perftools-dev')
        sudo('apt-get -y -qq install build-essential clang')
        sudo('apt-get -y -qq install libapr1-dev libaprutil1-dev')
        sudo('apt-get -y -qq install libboost-all-dev')
        sudo('apt-get -y -qq install libyaml-cpp-dev')
        sudo('apt-get -y -qq install python3-dev python3-pip')

@task
@roles('leaders')
@parallel
def config_ntp_leaders():
    sudo('apt-get -y -qq update')
    sudo('apt-get -y -qq dist-upgrade')
    sudo('apt-get -y -qq install ntp ntpstat')
    sudo('service ntp stop')
    ntp_template = Template(open('config/etc/ntp.conf').read())
    leader_ip = env.roledefs['leaders'][0]
    ntp_conf = StringIO(ntp_template.substitute(leader=leader_ip))
    Xput(ntp_conf, '/etc/ntp.conf', use_sudo=True)
    sudo('service ntp start')

@task
@roles('servers')
@parallel
def config_ntp_clients():
    sudo('apt-get -y -qq update')
    sudo('apt-get -y -qq dist-upgrade')
    sudo('apt-get -y -qq install ntpdate')
    leader_ip = env.roledefs['leaders'][0]
    run('sleep 2')
    sudo('ntpdate -q ' + leader_ip)

@task
@runs_once
@roles('leaders')
def build(args=None, clean=True):
    execute('retrieve_code')
#    execute('create_virtual_env')
    execute('install_apt_packages')
    with cd(env.nfs_home):
     run('sudo -H pip3 install -r requirements.txt')
     run('./waf configure build -M')
#    return
#    opts = ["./waf"] 
#    if args:
#        opts.insert(1, args)
#    if clean:
#        opts.extend(["distclean", "configure"])
#    opts.append("build")
#    with cd(env.nfs_home): 
#        run_python(' '.join(opts))

@task
@runs_once
@roles('leaders')
def retrieve_boost():
    return
    with cd(env.nfs_home):
        # get from beaker-1
        boost_archive = 'boost_1_58_0.tar.gz' 
        boost = 'http://216.165.108.10/~lamont/{boost_archive}'.format(**locals())
        run('wget {boost}'.format(**locals()))
        run('tar -xzvf {boost_archive}'.format(boost_archive=boost_archive))
    with cd(env.nfs_home + "/" + boost_archive.replace('.tar.gz','')):
        sudo('./bootstrap')
        sudo('./b2 install')

@task
@runs_once
@roles('leaders')
def retrieve_code():
    parent = os.path.dirname(env.nfs_home)
    with cd(parent):
        logging.info("check out code in {}".format(parent))

        if not exists(env.nfs_home):
            cmd = 'git clone --recursive {repo}'.format(repo=env.git_repo)
            run(cmd)
            with cd(env.nfs_home):
                cmd = 'git checkout {rev}'.format(rev=env.git_revision)
                run(cmd)
        else:
            with cd(env.nfs_home):
                run('git fetch origin')
                run('git checkout master')
                run('git pull origin master')
                run('git checkout {rev}'.format(rev=env.git_revision))
#        execute('retrieve_boost')

@task
@hosts('localhost')
def ping():
    execute('cluster.ping')

@task
@roles('leaders')
def download_archive():
    chars = 'ABCDEF'
    id = ''.join(random.choice(chars) for _ in range(10))
    archive_fn = "archive_{}.tgz".format(id)
    remote_path = "/tmp/{}".format(archive_fn)

    with cd('/home/ubuntu/janus/archive'):
        tar_cmd = 'tar -czvf {} .'.format(remote_path)
        run(tar_cmd)
        execute('cluster.download', remote_path)

    run('rm {}'.format(remote_path))


init()
