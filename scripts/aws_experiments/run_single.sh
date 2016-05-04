#!/bin/bash

function new_experiment {
	rm -rf tmp/* log/* 
	tar -czvf ~/${1}.tgz archive && rm -rf archive && mkdir -p archive
}

duration=40
prefix="single_dc"

exp_name=${prefix}_rw
./run_all.py -hh config/aws_hosts.yml -cc config/concurrent_10.yml -cc config/rw.yml -cc config/tpl_ww_paxos.yml -b rw_benchmark -m brq:brq -m 2pl_ww:multi_paxos -m occ:multi_paxos -m tapir:tapir -c 1 -c 2 -c 4 -c 8 -c 16 -c 32 -c 64 -s 1 -r 3 -d $duration $exp_name 
new_experiment $exp_name

exp_name=${prefix}_tpcc
./run_all.py -hh config/aws_hosts.yml -cc config/concurrent_10.yml -cc config/tpcc.yml -cc config/tapir.yml -b tpcc -m brq:brq -m 2pl_ww:multi_paxos -m occ:multi_paxos -m tapir:tapir -c 1 -c 2 -c 4 -c 8 -c 16 -c 32 -c 64 -s 3 -r 3 -d $duration $exp_name 
new_experiment $exp_name

zipfs=( 0.0 0.5 1.0 )
for zipf in "${zipfs[@]}"
do
	exp_name=${prefix}_tpca_zipf_${zipf}
	./run_all.py -hh config/aws_hosts.yml -cc config/concurrent_10.yml -cc config/tpca_zipf.yml -cc config/tapir.yml -b tpca -m brq:brq -m 2pl_ww:multi_paxos -m occ:multi_paxos -m tapir:tapir -c 1 -c 2 -c 4 -c 8 -c 16 -c 32 -z $zipf -s 3 -r 3 -d $duration $exp_name 
	new_experiment $exp_name
done
