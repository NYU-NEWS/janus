#!/usr/bin/env bash
# which mode, bench to run is in parameter.txt

source ./parameter.txt

# ---- sanity check ---- #
n_args=$#
if [[ ${n_args} -ne 5 ]]; then
    echo "run-eval.bash requires five arguments: 1. [mode], 2. [bench], 3. [n_cli], 4. [n_concurrent], and 5. [expt_dir]"
    exit
fi

if [[ "$1" != "none" ]] && [[ "$1" != "occ" ]] && [[ "$1" != "2pl" ]] && [[ "$1" != "acc" ]] && [[ "$1" != "acc_ss" ]]; then
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
n_cli=$3
n_concurrent=$4
expt_dir=$5

if [[ ! -d "${expt_dir}" ]]; then
    echo "expt_dir [${expt_dir}]  has not been created yet."
    exit
fi

if [[ ${mode} == "acc_ss"  ]]; then
    launch="${src_base}/run_ss.py"
fi

expt_name="${mode}-${bench}-${n_servers}s${n_cli}c${n_concurrent}n"
node_config="${n_servers}s${n_cli}c.yml"
benchmark_config="${bench}.yml"

echo "#### START NODE CONFIGS ####"
./config_nodes.bash ${mode} ${n_cli} ${n_concurrent}
echo "#### NODE CONFIGS FINISHED ####"
echo ""

sleep 3    # let the bullet fly a while

# run experiment trials
echo "#### START RUNNING EXPERIMENTS: BENCH (${bench}), MODE (${mode}) ####"
for trial in $(seq 1 ${trials}); do
    echo "---- trial ${trial} ----"
    while : ; do
        trial_dir="${expt_dir}/trial${trial}"
        mkdir -p ${trial_dir}
        ${launch} -n ${expt_name} -l ${trial_dir} -f ${config_dir}/${node_config} -f ${config_dir}/${benchmark_config}
        ./gather_data.bash ${trial_dir}
        sleep 3
        # --- check trial correctness --
        svr_error_count=$(cat ${trial_dir}/servers_err/* | wc -l)
        cli_error_count=$(cat ${trial_dir}/clients_err/* | wc -l)
        if [[ ${svr_error_count} -ne 0 ]] || [[ ${cli_error_count} -ne 0 ]]; then  # incorrect trial, redo
            sudo rm -r ${trial_dir}
            echo "---- trial ${trial} FAILED; now REDO ----"
        else    # correct trial, move on
            echo "---- trial ${trial} SUCCESS ----"
            break
        fi
        # ------------------------------
    done
done
echo "#### EXPERIMENTS: BENCH (${bench}), MODE (${mode}) FINISHED ####"