
### For Paxos

#### Multi-process paxos

```
make
python3 scripts/paxos-microbench.py -d 60 -f 'config/1c1s3r3p.yml' -f 'config/occ_paxos.yml' -t 30 -T 100000 -n 32 -P p3 -P p2 -P localhost
```

#### Multi-thread paxos

```
make
python3 scripts/paxos-microbench.py -d 60 -f 'config/1c1s3r1p.yml' -f 'config/occ_paxos.yml' -t 30 -T 100000 -n 32 -P localhost
```

#### Others

run `python3 scripts/paxos-microbench.py -h` for help  
**MUST** add all process names in the config files  
see the contents in config files for more information about paxos partition and replication
for multi-process paxos, if some site fails to start, change the order of your `-P` options and try again

#### To See CPU Profiling

PS: There are 32 outstanding requests.

```
./waf configure -p build
./build/microbench -d 60 -f 'config/1c1s3r1p.yml' -f 'config/occ_paxos.yml' -t 10 -T 5000 -n 32 -P localhost > ./log/proc-localhost.log
# if pprof is not installed in the system, try use scripts/pprof
pprof --pdf ./build/deptran_server process-localhost.prof > cpu.pdf
```

#### Microbench results

[Google Doc](https://docs.google.com/spreadsheets/d/1ANy2o1RQbw_gjPG1W3pTD3niqLZ8BfI8AwgxFGFBrO8/edit?usp=sharing)
