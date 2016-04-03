import logging
import os
import traceback
import time


from fabric.api import env, task, run, local, hosts
from fabric.api import execute, cd, runs_once, sudo
from fabric.contrib.files import exists, append
from fabric.decorators import roles
from fabric.context_managers import prefix

# tasks are exported under the module names
from pylib import ec2
from pylib import cluster

import pylib

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
        env.setdefault('git_repo', 'git@github.com:NYU-NEWS/janus.git')
        env.setdefault('git_revision', 'master')
        env.setdefault('py_virtual_env',
                       '{home}/py_venv'.format(home=env.nfs_home))


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
    if isinstance(regions, basestring):
        regions = regions.split(':')
    if isinstance(servers_per_region, basestring):
        servers_per_region = [ int(i) for i in servers_per_region.split(':') ]

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
        execute('retrieve_code')
        execute('build')
        execute('cluster.put_janus_config')

    except Exception as e:
        traceback.print_exc()
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
    if exists(env.py_virtual_env):
        return
    
    with cd(env.nfs_home):
        execute('retrieve_code')
        run('pyenv local 2.7.11')
        run('virtualenv {venv}'.format(venv=venv))
        run('{venv}/bin/pip install -r requirements.txt'.format(venv=venv))


@task
@runs_once
@roles('leaders')
def create_work_dirs():
    dirs = ['tmp/', 'logs/', 'log_archive/']
    for d in dirs:
        dir_path = os.path.join(env.nfs_home, d)
        run("mkdir -p {}".format(dir_path))

@task
@runs_once
@roles('leaders')
def build(args=None, clean=True):
    execute('retrieve_code')
    execute('create_virtual_env')
    sudo('apt-get install pkg-config')
    opts = ["./waf"] 
    if args:
        opts.insert(1, args)
    if clean:
        opts.extend(["distclean", "configure"])
    opts.append("build")
    with cd(env.nfs_home): 
        run_python(' '.join(opts))


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


init()
