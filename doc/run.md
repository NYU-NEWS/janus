
Note: this runs correctly only on Linux at the moment, not MacOS, because the command `ps` is different on the two platforms.

## Run a single trial

Use the run.py to run a single distributed trial.

run.py can be supplied with a configuration file:

```
./run.py -f config/sample.yml
```

run.py can also be supplied with multiple configuration files, so that when testing different combinations life is easier:

```
./run.py -f config/1c1s1p.yml -f config/tpl_ww.yml -f config/tpcc.yml -f config/concurrent_1.yml
```

## Run a batch of trials

We assume $janus is the root of this source repository.

The $janus/run_all.py file is the entry point for the distributed tests. Before this script can be used a configuration file describing the nodes in the cluster must be created.

To use run_all.py, a yaml file containing the nodes in the cluster should be provided. An example is below:

    host:
      host1: 1.2.3.4
      host2: 5.6.7.8
      host3: 9.10.11.12

The ./run_all.py program requires a -hh paramater that specifies the hosts in the cluster. Additionally, values for the number of servers (-s), clients (-c), and replicas (-r) should be provided. The required format is '[start]:[stop]:[step]' where the semantics are the same as the [python range function](https://docs.python.org/2/library/functions.html#range). With this information, the run_all.py will generate a configuration to map each server to a physical host machine in a round robin manner.

Two files in the scripts directory give examples of how to run the experiments in the paper: $janus/scripts/aws/run_single.sh and $janus/scripts/aws/run_multi.sh. These are for single and multiple datacenter scenarios respectively. They encode the set of parameters that are ultimately delivered to run_all.py. To run in your environment you will need a host config as outlined above, it can be located at the default place of '$janus/config/hosts.yml' or a custom location may be provided with the -hh paramater.

In addition to the hosts file and the number of servers and replicas, other config files from the $janus/config are required.

Consider the following command line:

    ./run_all.py -hh config/hosts-local.yml -s '1:4:1' -c '1:3:1' -r '3' -cc config/rw.yml -cc config/client_closed.yml -cc config/brq.yml -b rw -m brq:brq --allow-client-overlap testing

Where $janus/config/hosts-local.yml contains the following:
    
    host:
        localhost: localhost
    
This command specifies that we want to run with 1 to 3 shards, a replication level of 3, and 1 to 2 clients. We will run the Read/Write workload (1 key update) with the janus protocol (this is called brq for historical reasons). This gives a total of 6 separate experiments. One can specify multiple -cc <config-file> arguments, which will get aggregated together as one virtual config file. An example of the aggregated config file, $janus/config/sample.yml, is located in the config dir; or different options can be combined with multiple -cc options. The run_all.py script will generate a yaml file in $janus/tmp directory (with 'final' in the name) that can be used for debugging. The test results will be collected in the $PWD/archive directory, and the latest execution is in $PWD/log. run_all.py should be invoked from the $janus directory.

Possible values for the -m parameter are [listed here](https://github.com/NYU-NEWS/janus/blob/master/run_all.py#L23). Additional examples can be found in the $janus/scripts/aws/run_single.sh and $janus/scripts/aws/run_multi.sh files.

The scripts to build an aws environment, scripts/deploy_single.sh and scripts/deploy_multi.sh, will automatically generate the hosts file for the created ec2 instances.

## Debug tips

The output of a distributed trial will be included in the log directory.

Add this line to /etc/security/limits.conf (Ubuntu) to enable core dump globally. 
```
*  soft  core  unlimited
```

