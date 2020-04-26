#!/usr/bin/env bash
# this script runs all modes, all benchmarks, with all n_concurrent values
# specified in parameter.txt -- all all evals

source ./parameter.txt

expt_id=$(date +%s)  # a unique experiment id
expt_dir_base="${log_dir}/${expt_id}"
mkdir -p ${expt_dir_base}

for bench in ${benchmarks}; do  # run all benchmarks
    bench_dir="${expt_dir_base}/bench"
    mkdir -p ${bench_dir}
    for mode in ${modes}; do    # each bench runs all modes
        mode_dir="${bench_dir}/mode"
        mkdir -p ${mode_dir}
        for n_concurrent in ${n_concurrents}; do   # each mode varies n_concurrent
            n_concurrent_dir="${mode_dir}/n_concurrent_${n_concurrent}"
            mkdir -p ${n_concurrent_dir}
            ./run-eval.bash ${mode} ${bench} ${n_concurrent} ${n_concurrent_dir}
        done
    done
done