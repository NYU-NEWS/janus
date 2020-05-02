#!/usr/bin/env bash
# this script auto-config physical nodes for experiments, e.g., generate 1c1s1p.yml

source ./parameter.txt    # nodes are specified here

# ---- sanity check ---- #
n_args=$#
if [[ ${n_args} -ne 3 ]]; then
    echo "config_nodes.bash requires three arguments: 1. [mode], 2. [n_logical_clients], and 3. [n_concurrent]"
    exit
fi
# ---------------------- #

mode=$1
n_cli=$2
n_concurrent=$3

file="./configs/${n_servers}s${n_cli}c.yml"
rm -f ${file}

# --- print header --- #
echo "client:" >> ${file}
echo "  type: closed # { open, closed }" >> ${file}
echo "  rate: 1000 # only used for open clients -- units are txn/s" >> ${file}
echo " " >> ${file}

# --- print servers --- #
echo "site:" >> ${file}
echo "  server:" >> ${file}
# prepare servers
servers=""
for x in $(seq 1 ${n_servers}); do
    svr=$x
    if [[ $x -lt 10 ]]; then
        svr="0$x"
    fi
    server="s${x}01"
    port="81${svr}"
    svr_str="- [\"${server}:${port}\"]"
    echo "      ${svr_str}" >> ${file}
    servers="${servers} ${server}"
done
# prepare clients
echo "  client:" >> ${file}
clients=""
for x in $(seq 1 ${n_cli}); do
    client="c${x}"
    client_str="- [\"${client}\"]"
    echo "      ${client_str}" >> ${file}
    clients="${clients} ${client}"
done
# process
echo " " >> ${file}
echo "process:" >> ${file}
count=1
for svr in ${servers}; do
    echo "  ${svr}: svr-${count}" >> ${file}
    count=$((count + 1))
done
count=1
for cli in ${clients}; do
    echo "  ${cli}: cli-${count}" >> ${file}
    count=$((count + 1))
done
# physical node mapping
echo " " >> ${file}
echo "host:" >> ${file}
svr_count=1
while [[ ${svr_count} -le ${n_servers} ]]; do
    for svr in $(cat ${server_nodes}); do
        echo "  svr-${svr_count}: ${svr}" >> ${file}
        svr_count=$((svr_count + 1))
    done
done
cli_count=1
while [[ ${cli_count} -le ${n_cli} ]]; do
    for cli in $(cat ${client_nodes}); do
        echo "  cli-${cli_count}: ${cli}" >> ${file}
        cli_count=$((cli_count + 1))
    done
done
# --- mode ---
echo " " >> ${file}
echo "mode:" >> ${file}
if [[ "${mode}" == "acc_ss" ]] || [[ "${mode}" == "acc"  ]]; then
    echo "  cc: acc" >> ${file}
else
    echo "  cc: ${mode}" >> ${file}
fi
# echo "  cc: ${mode}" >> ${file}
echo "  ab: none" >> ${file}
echo "  read_only: occ" >> ${file}
echo "  batch: false" >> ${file}
echo "  retry: 9" >> ${file}
echo "  ongoing: 1" >> ${file}
# --- n_concurrent --
echo " " >> ${file}
echo "n_concurrent: ${n_concurrent}" >> ${file}

# --- copy this config file to nfs --- #
mkdir -p ${config_dir} && cp ${file} ${config_dir}