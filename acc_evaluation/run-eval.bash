#!/usr/bin/env bash
# which mode, bench to run is in parameter.txt

source ./parameter.txt

mkdir -p ${log_dir}
expt_name="${system}-${benchmark}-${n_servers}s${n_clients}c${n_concurrent}n"
node_config="${n_servers}s${n_clients}c.yml"
benchmark_config="${benchmark}.yml"

./config_nodes.bash
${launch} -n ${expt_name} -l ${log_dir} -f ${config_dir}${node_config} -f ${config_dir}${benchmark_config}