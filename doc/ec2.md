This project was tested using ubuntu 14.04 and Python version 2.7.11

###EC2 setup
Add your ssh public key to the ec2 images.
Then add these to ~/.ssh/config
(check [here](https://ip-ranges.amazonaws.com/ip-ranges.json) for EC2 ip ranges)

    Host 50.*.*.* # ec2 ip ranges
    User ubuntu
    Port 22
    IdentitiesOnly yes
    IdentityFile ~/.ssh/id_dsa

    Host 52.*.*.* # ec2 ip ranges
    User ubuntu
    Port 22
    IdentitiesOnly yes
    IdentityFile ~/.ssh/id_dsa


    Host 54.*.*.* # ec2 ip ranges
    User ubuntu
    Port 22
    IdentitiesOnly yes
    IdentityFile ~/.ssh/id_dsa

Generate usable aws credentials.
[link1](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html#cli-using-examples)
[link2](http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSGettingStartedGuide/AWSCredentials.html)

###System Setup
The following system packages are required to run the project:
TODO add later

###Python Setup
Install from source or using the [pyenv](https://github.com/yyuu/pyenv) tool. It's not recommended to use your system python.

The python dependencies are listed in the requirements.txt file at the root of the project. To install, make sure that [setuptools](https://pypi.python.org/pypi/setuptools), [virtualenv](https://pypi.python.org/pypi/virtualenv), and [pip](https://pypi.python.org/pypi/pip) are installed in your python environment. The pyenv tool listed above will automatically install these for each python version.

Once the tools above are installed, run the commands below from the root of the project to setup the environment for the application:

    pyenv local 2.7.11
    virtualenv .venv
    source .venv/bin/activate
    pip install -r requirements.txt

Activate the virtualenv before running commands: 

    source .venv/bin/activate

Or alternatively, run using the python in the virtualenv:

    .venv/bin/python <file>

###Deploy EC2 instances

fabfile.py contains commands to deploy to ec2, and manage instances. List available commands with:

    fab -l

The primary command to deploy is the fabric task 'deploy_all'. View this tasks' arguments with:
    
    fab -d deploy_all

For example, this command line will create 20 t2.small instances in us-west-2 region.

    fab deploy_all:regions=us-west-2,servers_per_region=20,instance_type=t2.small

Another example command line is: 

    fab deploy_all:regions=us-west-2:eu-west-1:ap-northeast-2,servers_per_region=5:4:4,instance_type=c4.large

Test connectivity with:

    fab ec2.set_instance_roles cluster.ping

If there are problems accessing the ~/janus dir then mount it on all servers with:

    fab ec2.set_instance_roles cluster.mount_nfs

To list all instances:

    fab ec2.set_instance_roles

To kill all instances:

    fab ec2.terminate_instances