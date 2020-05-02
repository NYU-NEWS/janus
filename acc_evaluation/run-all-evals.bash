#!/usr/bin/env bash
# this script runs all modes, all benchmarks, with all n_concurrent values
# specified in parameter.txt -- all all evals

source ./parameter.txt

expt_id=$(date +%s)  # a unique experiment id
expt_dir_base="${log_dir}/${expt_id}"
mkdir -p ${expt_dir_base}

# ---- sanity check ---- #
for bench in ${benchmarks}; do
    if [[ ! -f ${config_dir}/${bench}.yml ]]; then
        echo "config_dir: [${config_dir}] does not have: [${bench}.yml]. Terminates."
        exit
    fi
    if [[ ${bench} != "facebook" ]] && [[ ${bench} != "spanner" ]] && [[ ${bench} != "tpcc" ]]; then
        echo "This script only allows benchmarks: [facebook], [spanner], and [tpcc]. Terminates."
        echo "For dynamic workloads, please use [run-dynamic-evals.bash]"
        exit
    fi
done

for mode in ${modes}; do
    if [[ ${mode} != "none" ]] && [[ ${mode} != "occ" ]] && [[ ${mode} != "2pl" ]] && [[ ${mode} != "acc" ]] && [[ ${mode} != "acc_ss" ]]; then
        echo "This script only supports modes: [none], [occ], [2pl], and [acc]. Terminates."
        exit
    fi
done
# ---------------------- #

# ----- run eval expts ----- #
for bench in ${benchmarks}; do  # run all benchmarks
    bench_dir="${expt_dir_base}/${bench}"
    mkdir -p ${bench_dir}
    for mode in ${modes}; do    # each bench runs all modes
        mode_dir="${bench_dir}/${mode}"
        mkdir -p ${mode_dir}
        client=""
        for cli in ${n_clients}; do  # first we vary the number of logical clients
            client=${cli}
            n_client_dir="${mode_dir}/n_clients_${client}x1"
            mkdir -p ${n_client_dir}
            ./run-eval.bash ${mode} ${bench} ${client} 1 ${n_client_dir}
        done
        # now we have full # of logical clients, vary n_concurrent, starting with 2
        for n_concurrent in ${n_concurrents}; do
            n_concurrent_dir="${mode_dir}/n_clients_${client}x${n_concurrent}"
            mkdir -p ${n_concurrent_dir}
            ./run-eval.bash ${mode} ${bench} ${client} ${n_concurrent} ${n_concurrent_dir}
        done
    done
done
echo ""
echo "========================"
echo "  ALL EXPERIMENTS DONE"
echo "========================"
echo ""
# -------------------------- #

sleep 10

# -------- process data --------- #
./process-data.bash ${expt_id}

echo "======== !EVALUATION FINISHED! ========"