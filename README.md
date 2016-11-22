
## Janus

Code repo for our OSDI '16 paper:
[Consolidating Concurrency Control and Consensus for Commits under Conflicts](http://mpaxos.com/pub/janus-osdi16.pdf)


## Quick start (with Ubuntu 16.04)

Install dependencies:

```
sudo apt-get update
sudo apt-get install -y build-essential
sudo apt-get install -y clang
sudo apt-get install -y libapr1-dev libaprutil1-dev
sudo apt-get install -y libboost-all-dev
sudo apt-get install -y libyaml-cpp-dev
sudo apt-get install -y python-dev
sudo apt-get install -y python-pip
sudo pip install -r requirements.txt
```

Get source code:
```
git clone --recursive https://github.com/NYU-NEWS/janus.git
```

Build:

```
./waf configure build -t

```
Test run:
```
./test_run.py -m janus
```

## More
Check the [wiki page](https://github.com/NYU-NEWS/janus/wiki) to find more about how to build the system on older or newer distros, how to run the system in a distributed setup, and how to generate figures in the paper, etc.
