
## Janus 
[![Build Status](https://travis-ci.org/NYU-NEWS/janus.svg?branch=master)](https://travis-ci.org/NYU-NEWS/janus)

Code repo for our OSDI '16 paper:
[Consolidating Concurrency Control and Consensus for Commits under Conflicts](http://mpaxos.com/pub/janus-osdi16.pdf)


## Quick start (with Ubuntu 16.04 or newer)

Install dependencies:

```
sudo apt-get update
sudo apt-get install -y \
    git \
    pkg-config \
    build-essential \
    clang \
    libapr1-dev libaprutil1-dev \
    libboost-all-dev \
    libyaml-cpp-dev \
    python3-dev \
    python3-pip \
    libgoogle-perftools-dev
sudo pip install -r requirements.txt
```

Get source code:
```
git clone --recursive https://github.com/NYU-NEWS/janus.git
```

Build:

```
python3 waf configure build -t

```

## More
Check out the doc directory to find more about how to build the system on older or newer distros, how to run the system in a distributed setup, and how to generate figures in the paper, etc.
<!-- 
## Do some actual good
For every star collected on this project, I will make a $25 charity loan via [Kiva] (https://www.kiva.org/invitedby/gzcdm3147?utm_campaign=permurl-share-invite-normal&utm_medium=referral&utm_content=gzcdm3147&utm_source=mpaxos.com).
-->

### Run paxos only

#### Potential Environment Problems

* `python3: error while loading shared libraries: libpython3.5m.so.1.0: cannot open shared object file`  
    download `libpython3.5m.so.1.0`  
    ```
    cd /etc/ld.so.conf.d
    vim python3.conf
    ```
    add the path where the file is saved  
    ```
    ldconfig
    ```
* `error while loading shared libraries: libdeptran_server.so: cannot open shared object file: No such file or directory`
    ```
    export LD_LIBRARY_PATH=$(pwd)/build
    ```

#### One-site paxos
```
nohup ./build/deptran_server -b -d 60 -f 'config/1c1s1p.yml' -f 'config/occ_paxos.yml' -t 10 -T 100000 -n 32 -P localhost > ./log/proc-localhost.log
```

#### Multi-thread paxos
```
nohup ./build/deptran_server -b -d 60 -f 'config/1c1s3r1p.yml' -f 'config/occ_paxos.yml' -t 10 -T 100000 -n 32 -P localhost > ./log/proc-localhost.log
```

[new]()  
#### Multi-process paxos  
open thress shells  
on the fist one, run  
```
nohup ./build/deptran_server -b -d 60 -f 'config/1c1s3r3p.yml' -f 'config/occ_paxos.yml' -t 10 -T 100000 -n 32 -P localhost > ./log/proc-localhost.log
```
on the second one, run
```
nohup ./build/deptran_server -b -d 60 -f 'config/1c1s3r3p.yml' -f 'config/occ_paxos.yml' -t 10 -T 100000 -n 32 -P p2 > ./log/proc-p2.log
```

on the third one, run
```
nohup ./build/deptran_server -b -d 60 -f 'config/1c1s3r3p.yml' -f 'config/occ_paxos.yml' -t 10 -T 100000 -n 32 -P p3 > ./log/proc-p3.log
```

#### To See CPU Profiling

PS: There are 32 outstanding requests.

```
./waf-2.0.18 configure -p build
./build/deptran_server -d 60 -f 'config/1c1s3r1p.yml' -f 'config/occ_paxos.yml' -t 10 -T 5000 -n 32 -P localhost > ./log/proc-localhost.log
./pprof --pdf ./build/deptran_server process-localhost.prof > cpu.pdf
```

#### Microbench results

[Google Doc](https://docs.google.com/spreadsheets/d/1ANy2o1RQbw_gjPG1W3pTD3niqLZ8BfI8AwgxFGFBrO8/edit?usp=sharing)
