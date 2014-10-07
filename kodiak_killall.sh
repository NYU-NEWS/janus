#/bin/bash

EMULAB_LIST=`/share/probe/bin/emulab-listall`
IFS=',' read -a ALL_NODES <<<"${EMULAB_LIST}"

for serv in ${ALL_NODES[@]}
do
    ssh ${serv}.deptran-scale.Spartan 'killall -9 deptran_server 2>/dev/null'
    ssh ${serv}.deptran-scale.Spartan 'killall -9 deptran_client 2>/dev/null'
done
