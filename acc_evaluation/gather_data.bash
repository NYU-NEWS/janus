#!/usr/bin/env bash
# this script scp all logs from servers and clients to the
# control node after each trial

source ./parameter.txt

# ---- sanity check ---- #
n_args=$#
if [[ ${n_args} -ne 1 ]]; then
    echo "gather_data.bash requires 1 argument: 1. [trial_dir]"
    exit
fi
# ---------------------- #

trial_dir=$1
if [[ ! -d "${trial_dir}" ]]; then
    echo "trial_dir [${trial_dir}]  has not been created yet."
    exit
fi

echo "---- gathering trial data ----"

servers_log="${trial_dir}/servers_log/"
servers_err="${trial_dir}/servers_err/"
clients_log="${trial_dir}/clients_log/"
clients_err="${trial_dir}/clients_err/"

mkdir -p ${servers_log}
mkdir -p ${servers_err}
mkdir -p ${clients_log}
mkdir -p ${clients_err}

sleep 3   # let the bullet fly

# ---- get logs and errs from servers ---- #
for svr in $(cat ${server_nodes}); do
    scp ${svr}:${trial_dir}/*.log ${servers_log}
    scp ${svr}:${trial_dir}/*.err ${servers_err}
done

# ---- get logs and errs from clients ---- #
for cli in $(cat ${client_nodes}); do
    scp ${cli}:${trial_dir}/*.log ${clients_log}
    scp ${cli}:${trial_dir}/*.err ${clients_err}
done

echo "---- gathering trial data finished ----"