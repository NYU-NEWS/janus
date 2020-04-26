#!/usr/bin/env bash
# which mode, bench to run is in parameter.txt

source ./parameter.txt

# ---- sanity check ---- #
n_args=$($#)
if [[ ${n_args} -ne 4 ]]; then
    echo "run-eval.bash requires four arguments: 1. [mode], 2. [bench], 3. [n_concurrent], and 4. [expt_dir]"
    exit
fi

if [[ "$1" != "none" ]] && [[ "$1" != "occ" ]] && [[ "$1" != "2pl" ]] && [[ "$1" != "acc" ]]; then
    echo "Invalid [mode] argument: only supports [none], [occ], [2pl], and [occ] for now."
    exit
fi

if [[ "$2" != "facebook" ]] && [[ "$2" != "spanner" ]] && [[ "$2" != "tpcc" ]] && [[ "$2" != "dynamic" ]]; then
    echo "Invalid [bench] argument: only supports [facebook], [spanner], [tpcc], and [dynamic] for now."
    exit
fi
# ---------------------- #

mode=$1
bench=$2
n_concurrent=$3
expt_dir=$4

if [[ ! -d "${expt_dir}" ]]; then
    echo "expt_dir [${expt_dir}]  has not been created yet."
    exit
fi

expt_name="${mode}-${bench}-${n_servers}s${n_clients}c${n_concurrent}n"
node_config="${n_servers}s${n_clients}c.yml"
benchmark_config="${bench}.yml"

./config_nodes.bash ${mode} ${n_concurrent}

sleep 3    # let the bullet fly a while

# run experiment trials
for trial in $(seq 1 ${trials}); do
    while : ; do
        trial_dir="${expt_dir}/trial${trial}"
        mkdir -p ${trial_dir}
        ${launch} -n ${expt_name} -l ${trial_dir} -f ${config_dir}/${node_config} -f ${config_dir}/${benchmark_config}
        ./gather_data.bash ${trial_dir}
        sleep 3
        # --- check trial correctness --
        error_count=$(cat ${trial_dir}/servers_err/* | wc -l)
        if [[ ${error_count} -ne 0 ]]; then  # incorrect trial, redo
            sudo rm -r ${trial_dir}
        else    # correct trial, move on
            break
        fi
        # ------------------------------
    done
done