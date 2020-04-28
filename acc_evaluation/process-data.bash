#!/usr/bin/env bash
# process data when all expts are done
# this is for facebook, spanner, and tpcc benchmarks, not for dynamic
# this will produce data for plotting latency-throughput graphs

source ./parameter.txt

echo ""
echo "-------- Processing data --------"

# ---- sanity check ---- #
n_args=$#
if [[ ${n_args} -ne 1 ]]; then
    echo "process-data.bash requires one argument: 1. [expt_id]"
    echo ""
    echo "-------- Processing data FAILED --------"
    exit
fi
# ---------------------- #

expt_id=$1
expt_dir="${log_dir}/${expt_id}"
data_dir="${expt_dir}/eval_results"
mkdir -p ${data_dir}

median_trial=$((trials / 2 + 1))

for bench in ${benchmarks}; do
    bench_data_dir="${data_dir}/${bench}_data"
    mkdir -p ${bench_data_dir}
    latency_throughput_output="${bench_data_dir}/${bench}-latency-throughput.txt"
    commit_rate_output="${bench_data_dir}/${bench}-commit-rate.txt"
    latency_throughput_file="${bench_data_dir}/${bench}-latency-throughput-graph.txt"
    throughput_file="${bench_data_dir}/${bench}-throughput-graph.txt"
    latency_file="${bench_data_dir}/${bench}-latency-graph.txt"
    commit_rate_file="${bench_data_dir}/${bench}-commit-rate-graph.txt"
    # data_detail="${bench_data_dir}/${bench}-more-details.txt"
    bench_dir="${expt_dir}/${bench}"

    client=""
    for cli in ${n_clients}; do
        client=${cli}
        echo "${cli}" >> ${latency_throughput_output}
        echo "${cli}" >> ${commit_rate_output}
        echo "${cli}" >> ${throughput_file}
        echo "${cli}" >> ${latency_file}
        echo "${cli}" >> ${commit_rate_file}
    done
    for n_concur in ${n_concurrents}; do
        n_cli=$((n_concur * client))
        echo "${n_cli}" >> ${latency_throughput_output}
        echo "${n_cli}" >> ${commit_rate_output}
        echo "${n_cli}" >> ${throughput_file}
        echo "${n_cli}" >> ${latency_file}
        echo "${n_cli}" >> ${commit_rate_file}
    done

    for mode in ${modes}; do
        mode_dir="${bench_dir}/${mode}"
        mode_latency_throughput_file="${bench_data_dir}/${mode}-latency-throughput.txt"
        mode_commit_rate_file="${bench_data_dir}/${mode}-commit-rate.txt"
        mode_tmp_file1="${bench_data_dir}/${mode}-tmp1.txt"
        mode_tmp_file2="${bench_data_dir}/${mode}-tmp2.txt"
        cl=""
        for c in ${n_clients}; do
            cl=${c}
            concurrent_dir="${mode_dir}/n_clients_${cl}x1"
            trials_tmp_file="${bench_data_dir}/${mode}-n_clients_${cl}x1-trials.txt"
            for trial in $(seq 1 ${trials}); do
                trial_dir="${concurrent_dir}/trial${trial}"
                data_file="${trial_dir}/${mode}-${bench}-${n_servers}s${cl}c1n.log"
                if [[ ! -f ${data_file} ]]; then
                    echo "Data file: [${data_file}] does not exist! TERMINATES."
                    exit
                fi
                if [[ ${bench} == "facebook" ]] || [[ ${bench} == "spanner" ]]; then
                    throughput=$(cat ${data_file} | grep "tps:" | awk -F " " '{SUM+=$2}END{print SUM}')
                    rotxn_latency=$(cat ${data_file} | grep -w "latency:" | head -1 | awk -F "," {'print $1'} | awk -F " " {'print $3'})
                    total=$(cat ${data_file} | grep "start_cnt:" | awk -F " " '{SUM+=$2}END{print SUM}')
                    commits=$(cat ${data_file} | grep "commits:" | awk -F " " '{SUM+=$2}END{print SUM}')
                    commit_rate=$(( commits * 100 / total))
                    echo "trial${trial}: ${throughput} ${rotxn_latency} ${total} ${commits} ${commit_rate}" >> ${trials_tmp_file}
                elif [[ ${bench} == "tpcc" ]]; then
                    new_order_thruput=$(cat ${data_file} | grep "tps:" | head -1 | awk -F " " {'print $2'})
                    new_order_latency=$(cat ${data_file} | grep -w "latency:" | head -1 | awk -F "," {'print $1'} | awk -F " " {'print $3'})
                    total=$(cat ${data_file} | grep "start_cnt:" | head -1 | awk -F " " {'print $2'})
                    commits=$(cat ${data_file} | grep "commits:" | head -1 | awk -F " " {'print $2'})
                    commit_rate=$(( commits * 100 / total))
                    echo "trial${trial}: ${new_order_thruput} ${new_order_latency} ${total} ${commits} ${commit_rate}" >> ${trials_tmp_file}
                else
                    echo "Unspported benchmark: [${bench}]. Data processing terminates."
                    exit
                fi
            done
            # now find the median trial (by throughput)
            throughput_latency=$(cat ${trials_tmp_file} | sort -k2 -n | sed "${median_trial}q;d" | awk -F " " {'print $2 " " $3'})
            commit_rate=$(cat ${trials_tmp_file} | sort -k2 -n | sed "${median_trial}q;d" | awk -F " " {'print $6'})
            echo "${throughput_latency}" >> ${mode_latency_throughput_file}
            echo "${commit_rate}" >> ${mode_commit_rate_file}
        done
        for n_concurrent in ${n_concurrents}; do
            concurrent_dir="${mode_dir}/n_clients_${cl}x${n_concurrent}"
            trials_tmp_file="${bench_data_dir}/${mode}-n_clients_${cl}x${n_concurrent}-trials.txt"
            for trial in $(seq 1 ${trials}); do
                trial_dir="${concurrent_dir}/trial${trial}"
                data_file="${trial_dir}/${mode}-${bench}-${n_servers}s${cl}c${n_concurrent}n.log"
                if [[ ! -f ${data_file} ]]; then
                    echo "Data file: [${data_file}] does not exist! TERMINATES."
                    exit
                fi
                if [[ ${bench} == "facebook" ]] || [[ ${bench} == "spanner" ]]; then
                    throughput=$(cat ${data_file} | grep "tps:" | awk -F " " '{SUM+=$2}END{print SUM}')
                    rotxn_latency=$(cat ${data_file} | grep -w "latency:" | head -1 | awk -F "," {'print $1'} | awk -F " " {'print $3'})
                    total=$(cat ${data_file} | grep "start_cnt:" | awk -F " " '{SUM+=$2}END{print SUM}')
                    commits=$(cat ${data_file} | grep "commits:" | awk -F " " '{SUM+=$2}END{print SUM}')
                    commit_rate=$(( commits * 100 / total))
                    echo "trial${trial}: ${throughput} ${rotxn_latency} ${total} ${commits} ${commit_rate}" >> ${trials_tmp_file}
                elif [[ ${bench} == "tpcc" ]]; then
                    new_order_thruput=$(cat ${data_file} | grep "tps:" | head -1 | awk -F " " {'print $2'})
                    new_order_latency=$(cat ${data_file} | grep -w "latency:" | head -1 | awk -F "," {'print $1'} | awk -F " " {'print $3'})
                    total=$(cat ${data_file} | grep "start_cnt:" | head -1 | awk -F " " {'print $2'})
                    commits=$(cat ${data_file} | grep "commits:" | head -1 | awk -F " " {'print $2'})
                    commit_rate=$(( commits * 100 / total))
                    echo "trial${trial}: ${new_order_thruput} ${new_order_latency} ${total} ${commits} ${commit_rate}" >> ${trials_tmp_file}
                else
                    echo "Unspported benchmark: [${bench}]. Data processing terminates."
                    exit
                fi
            done
            # now find the median trial (by throughput)
            throughput_latency=$(cat ${trials_tmp_file} | sort -k2 -n | sed "${median_trial}q;d" | awk -F " " {'print $2 " " $3'})
            commit_rate=$(cat ${trials_tmp_file} | sort -k2 -n | sed "${median_trial}q;d" | awk -F " " {'print $6'})
            echo "${throughput_latency}" >> ${mode_latency_throughput_file}
            echo "${commit_rate}" >> ${mode_commit_rate_file}
        done
        paste ${latency_throughput_output} ${mode_latency_throughput_file} > ${mode_tmp_file1}
        mv ${mode_tmp_file1} ${latency_throughput_output}
        paste ${commit_rate_output} ${mode_commit_rate_file} > ${mode_tmp_file2}
        mv ${mode_tmp_file2} ${commit_rate_output}
    done
    # now we make throughput, latency, and commit-rate data out of output files
    # -- throughput
    throughput_tmp="${bench_data_dir}/${bench}-throughput.tmp"
    client_tmp="${bench_data_dir}/${bench}-client.tmp"
    col=2
    for mode in ${modes}; do
        cat ${latency_throughput_output} | awk -v c="${col}" '{ print $c/$2}' > ${throughput_tmp}
        paste ${throughput_file} ${throughput_tmp} > ${client_tmp}
        mv ${client_tmp} ${throughput_file}
        col=$((col + 2))
    done
    # -- latency
    latency_tmp="${bench_data_dir}/${bench}-latency.tmp"
    col=3
    for mode in ${modes}; do
        cat ${latency_throughput_output} | awk -v c="${col}" '{ print $c/$3}' > ${latency_tmp}
        paste ${latency_file} ${latency_tmp} > ${client_tmp}
        mv ${client_tmp} ${latency_file}
        col=$((col + 2))
    done
    # -- commit rate
    commit_tmp="${bench_data_dir}/${bench}-commit.tmp"
    col=2
    for mode in ${modes}; do
        cat ${commit_rate_output} | awk -v c="${col}" '{ print $c/100}' > ${commit_tmp}
        paste ${commit_rate_file} ${commit_tmp} > ${client_tmp}
        mv ${client_tmp} ${commit_rate_file}
        col=$((col + 1))
    done
    # -- clean up
    cp ${latency_throughput_output} ${latency_throughput_file}
    rm ${throughput_tmp}
    rm ${latency_tmp}
    rm ${commit_tmp}
done

echo ""
echo "-------- Processing data FINISHED --------"