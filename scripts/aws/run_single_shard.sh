#!/bin/bash
./run_all.py single_shard -hh config/aws_hosts.yml -cc config/concurrent_1000.yml -cc config/janus.yml -cc config/rw_fixed.yml -cc config/client_open.yml -m janus:janus -m tapir:tapir -m 2pl_ww:multi_paxos -m occ:multi_paxos -b rw_benchmark -cl 277 554 833 1111 1667 2222 2777 3333 3888 -c 18 -s 1 -r 1
