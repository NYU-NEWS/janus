
This project was recently (2019 Nov) tested using Ubuntu 18.04

These steps need to be executed on a Ubuntu machine that has a good connection to EC2. On this machine there needs to be an account named `ubuntu`; enable password-free ssh connection to localhost by: 

```
ssh-copy-id -i config/ssh/id_rsa ubuntu@localhost
```

## Setup EC2 credentials
First, import the ssh public key `config/ssh/id_rsa.pub` to EC2 (repeat for every data center used in the test), ane name it as "test_key".

If you are deploying code from your own repo, make sure to add the same public key to your repo. 

Next, set up credentials (in e.g. ~/.aws/credentials):

```
[default]
aws_access_key_id = YOUR_KEY
aws_secret_access_key = YOUR_SECRET
```

Follow this [link](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html#cli-using-examples) 
to create key and secret.

## Deploy EC2 instances


`fabfile.py` contains commands to deploy to ec2, and manage instances. List available commands with:

```
fab -l
```

The primary command to deploy is the fabric task `deploy_all`. View this tasks' arguments with:
    
```
fab -d deploy_all
```

For example, this command line will create 20 t2.small instances in us-west-2 region.

```
fab deploy_all:regions=us-west-2,servers_per_region=20,instance_type=t2.small
```

Another example command line is: 

```
fab deploy_all:regions=us-west-2:eu-west-1:ap-northeast-2,servers_per_region=5:4:4,instance_type=c4.large
```
Note: EC2 accounts have quota for how many VMs you can launch in each data center; it can be increased by contacting AWS.

Test connectivity with:

```
fab ec2.set_instance_roles cluster.ping
```

If there are problems accessing the ~/janus dir then mount it on all servers with:

```
fab ec2.set_instance_roles cluster.mount_nfs
```

To list all instances:

```
fab ec2.set_instance_roles
```

To kill all instances:

```
fab ec2.terminate_instances
```

## Python Setup (deprecated)
Install from source or using the [pyenv](https://github.com/yyuu/pyenv) tool. It's not recommended to use your system python.

The python dependencies are listed in the requirements.txt file at the root of the project. To install, make sure that [setuptools](https://pypi.python.org/pypi/setuptools), [virtualenv](https://pypi.python.org/pypi/virtualenv), and [pip](https://pypi.python.org/pypi/pip) are installed in your python environment. The pyenv tool listed above will automatically install these for each python version.

Once the tools above are installed, run the commands below from the root of the project to setup the environment for the application:

```
pyenv local 2.7.11
virtualenv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Activate the virtualenv before running commands: 

```
source .venv/bin/activate
```

Or alternatively, run using the python in the virtualenv:

```
.venv/bin/python <file>
```
