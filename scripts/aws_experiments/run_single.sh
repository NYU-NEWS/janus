#!/bin/bash
duration=90
prefix="single_dc"

base_options="-g -hh config/aws_hosts.yml -cc config/concurrent_10.yml -cc config/rw_fixed.yml -cc config/tpl_ww_paxos.yml -d $duration"
protocols="-m brq:brq -m 2pl_ww:multi_paxos -m occ:multi_paxos -m tapir:tapir"
client_nums="-c 1 -c 2 -c 4 -c 8 -c 16 -c 32"
replica_options="-s 1 -r 3"

function archive_experiment {
	rm -rf tmp/* log/* 
	tar -czvf ~/${1}_graphs.tgz archive/*eps
	tar -czvf ~/${1}.tgz archive && rm -rf archive && mkdir -p archive
}

function run_experiment() {
	suffix=$1
	benchmark=$2
	
	name=${prefix}_${suffix}
	echo ./run_all.py -b $benchmark $base_options $protocols $client_nums $replica_options $replica_options $name 
	eval ./run_all.py -b $benchmark $base_options $protocols $client_nums $replica_options $replica_options $name 
	archive_experiment $name
}

exp_name=${prefix}_zipf_graph
scripts/aws_experiments/zipf_graph.py $exp_name -c 64 -f config/tpca_zipf.yml config/tapir.yml config/concurrent_10.yml
new_experiment $exp_name

run_experiment "rw_fixed" "rw_benchmark"
run_experiment "rw" "rw_benchmark"
run_experiment "tpcc" "tpcc"

zipfs=( 0.0 0.25 0.5 0.75 1.0 )
for zipf in "${zipfs[@]}"
do
	run_experiment "tpca_zipf_${zipf}" "tpca"
done
