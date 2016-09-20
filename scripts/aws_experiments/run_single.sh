#!/bin/bash

function new_experiment {
	rm -rf tmp/* log/* 
	tar -czvf ~/${1}.tgz archive && rm -rf archive && mkdir -p archive
}

duration=90
prefix="single_dc"

exp_name=${prefix}_zipf_graph
scripts/aws_experiments/zipf_graph.py $exp_name -c 64 -f config/client_closed.yml config/tpca_zipf.yml config/tapir.yml config/concurrent_10.yml
new_experiment $exp_name

exp_name=${prefix}_rw_fixed
./run_all.py -u 2 -hh config/aws_hosts.yml -cc config/client_closed.yml -cc config/concurrent_10.yml -cc config/rw_fixed.yml -cc config/tpl_ww_paxos.yml -b rw_benchmark -m brq:brq -m 2pl_ww:multi_paxos -m occ:multi_paxos -m tapir:tapir -c 1 -c 2 -c 4 -c 8 -c 16 -c 32 -s 1 -r 3 -d $duration $exp_name 
new_experiment $exp_name

exp_name=${prefix}_rw
./run_all.py -u 2 -hh config/aws_hosts.yml -cc config/client_closed.yml -cc config/concurrent_10.yml -cc config/rw.yml -cc config/tpl_ww_paxos.yml -b rw_benchmark -m brq:brq -m 2pl_ww:multi_paxos -m occ:multi_paxos -m tapir:tapir -c 1 -c 2 -c 4 -c 8 -c 16 -c 32 -s 1 -r 3 -d $duration $exp_name 
new_experiment $exp_name

exp_name=${prefix}_tpcc
./run_all.py -u 2 -hh config/aws_hosts.yml -cc config/client_closed.yml -cc config/concurrent_10.yml -cc config/tpcc.yml -cc config/tapir.yml -b tpcc -m brq:brq -m 2pl_ww:multi_paxos -m occ:multi_paxos -m tapir:tapir -c 1 -c 2 -c 4 -c 8 -c 16 -c 32 -s 10 -r 3 -d $duration $exp_name 
new_experiment $exp_name

exp_name=${prefix}_tpca_fixed
./run_all.py -u 2 -hh config/aws_hosts.yml -cc config/client_closed.yml -cc config/concurrent_10.yml -cc config/tpca_fixed.yml -cc config/tapir.yml -b tpca -m brq:brq -m 2pl_ww:multi_paxos -m occ:multi_paxos -m tapir:tapir -c 1 -c 2 -c 4 -c 8 -c 16 -c 32 -c 64 -s 6 -r 3 -d $duration $exp_name
new_experiment $exp_name

zipfs=( 0.0 0.25 0.5 0.75 1.0 )
for zipf in "${zipfs[@]}"
do
	exp_name=${prefix}_tpca_zipf_${zipf}
	./run_all.py -u 2 -hh config/aws_hosts.yml -cc config/client_closed.yml -cc config/concurrent_10.yml -cc config/tpca_zipf.yml -cc config/tapir.yml -b tpca -m brq:brq -m 2pl_ww:multi_paxos -m occ:multi_paxos -m tapir:tapir -c 1 -c 2 -c 4 -c 8 -c 16 -c 32 -c 64 -z $zipf -s 6 -r 3 -d $duration $exp_name
	new_experiment $exp_name
done

