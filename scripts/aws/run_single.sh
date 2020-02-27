#!/bin/bash
duration=30
concurrent=100
./waf configure build 
prefix="single_dc"

set -v

function run_tests {
	write_concurrent

#	tpca_fixed 3
	zipf_graph 3
	tpcc 6 

#	rw_fixed
#	tpca_fixed
#	tpcc
#	zipf_graph_open 6
#	zipf_graph 1
#	zipf_graph 3
#	zipf_graph 6
#	rw
#	zipfs
}

function write_concurrent {
	echo -e "n_concurrent: $concurrent\n" > /tmp/concurrent.yml
}

function new_experiment {
	rm -rf tmp/* log/* 
	# tar -czvf ~/${1}.tgz archive && rm -rf archive && mkdir -p archive
	printf '=%.0s' {1..40}
	echo
	echo "end $1"
	printf '=%.0s' {1..40}
	echo
}

function zipf_graph {
	shards=$1
	if [[ shards -le 3 ]]
	then
		cpu=1
	else
		cpu=2
	fi
	exp_name=${prefix}_zipf_graph_${shards}
	scripts/aws/zipf_graph.py $exp_name -s $shards -u $cpu -c 9 -d $duration -f config/client_closed.yml config/tpca_zipf.yml config/tapir.yml /tmp/concurrent.yml
	new_experiment $exp_name
}

function zipf_graph_open {
	shards=$1
	if [[ shards -le 3 ]]
	then
		cpu=1
	else
		cpu=2
	fi
	exp_name=${prefix}_zipf_graph_open_${shards}
	scripts/aws/zipf_graph.py $exp_name -s $shards -u $cpu -c 9 -d $duration -f config/client_open.yml config/tpca_zipf.yml config/tapir.yml /tmp/concurrent.yml -cl 1111
	new_experiment $exp_name
}

function rw_fixed {
	exp_name=${prefix}_rw_fixed
	./run_all.py -g -hh config/aws_hosts.yml -cc config/client_closed.yml -cc /tmp/concurrent.yml -cc config/rw_fixed.yml -cc config/tpl_ww_paxos.yml -b rw_benchmark -m brq:brq -m 2pl_ww:multi_paxos -m occ:multi_paxos -m tapir:tapir -c 1 -c 2 -c 4 -c 8 -c 12 -c 16 -s 1 -u 1 -r 3 -d $duration $exp_name 
	new_experiment $exp_name
}

function rw {
	exp_name=${prefix}_rw
	./run_all.py -g -hh config/aws_hosts.yml -cc config/client_closed.yml -cc /tmp/concurrent.yml -cc config/rw.yml -cc config/tpl_ww_paxos.yml -b rw_benchmark -m brq:brq -m 2pl_ww:multi_paxos -m occ:multi_paxos -m tapir:tapir -c 1 -c 2 -c 4 -c 8 -c 16 -s 1 -u 1 -r 3 -d $duration $exp_name 
	new_experiment $exp_name
}

function tpcc {
	shards=$1
	if [[ shards -le 3 ]]
	then
		cpu=1
	else
		cpu=2
	fi
	exp_name=${prefix}_tpcc_${shards}
	./run_all.py -g -hh config/aws_hosts.yml -cc config/client_closed.yml -cc /tmp/concurrent.yml -cc config/tpcc.yml -cc config/tapir.yml -b tpcc -m brq:brq -m troad:troad -c 1 -c 2 -c 4 -c 8 -c 16 -c 24 -c 28 -c 32 -s $shards -u $cpu -r 3 -d $duration $exp_name --allow-client-overlap
#	./run_all.py -g -hh config/aws_hosts.yml -cc config/client_closed.yml -cc /tmp/concurrent.yml -cc config/tpcc.yml -cc config/tapir.yml -b tpcc -m troad:troad -c 1 -c 2 -c 4 -c 8 -c 16 -c 24 -c 28 -c 32 -s $shards -u $cpu -r 3 -d $duration $exp_name --allow-client-overlap
	new_experiment $exp_name
}

function tpca_fixed {
	shards=$1
	if [[ shards -le 3 ]]
	then
		cpu=1
	else
		cpu=2
	fi
	exp_name=${prefix}_tpca_fixed
	./run_all.py -g -hh config/aws_hosts.yml -cc config/client_closed.yml -cc /tmp/concurrent.yml -cc config/tpca_fixed.yml -cc config/tapir.yml -b tpca -m brq:brq -m 2pl_ww:multi_paxos -m occ:multi_paxos -m tapir:tapir -c 1 -c 2 -c 4 -c 8 -c 12 -c 16 -s $shards -u $cpu -r 3 -d $duration $exp_name
	new_experiment $exp_name
}

function zipfs {
	zipfs=( 0.0 0.25 0.5 0.75 1.0 )
	for zipf in "${zipfs[@]}"
	do
		exp_name=${prefix}_tpca_zipf_${zipf}
		./run_all.py -g -hh config/aws_hosts.yml -cc config/client_closed.yml -cc /tmp/concurrent.yml -cc config/tpca_zipf.yml -cc config/tapir.yml -b tpca -m brq:brq -m 2pl_ww:multi_paxos -m occ:multi_paxos -m tapir:tapir -c 1 -c 2 -c 4 -c 8 -c 16 -z $zipf -s 6 -u 2 -r 3 -d $duration $exp_name
		new_experiment $exp_name
	done
}

run_tests

./waf configure build -W
prefix="multi_dc"
run_tests
