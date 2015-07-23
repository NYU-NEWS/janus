#/bin/bash

FILE_DIR=`dirname ${BASH_SOURCE[0]}`
cd ${FILE_DIR}

FULL_PATH=`pwd`

# >>>>>>>> SERVER OPTIONS >>>>>>>>
# total number of servers
NUM_SERVS=8

# number of servers to run on each machine
SERVS_PER_MACHINE=2

# server port
SERV_START_PORT=23000

# server machines
#SERV_MACHINES=( beaker-20 beaker-21 beaker-22 beaker-23 beaker-24 beaker-25 )
#SERV_MACHINES=( beaker-22 beaker-23 beaker-24 beaker-25 )
SERV_MACHINES=( beaker-22 beaker-23 beaker-24 beaker-25 )
# <<<<<<<< SERVER OPTIONS <<<<<<<<



# >>>>>>>> CLIENT OPTIONS >>>>>>>>
# number of coordinators for each client process
COOS_PER_CLIENT=1

CLIENT_PROCESS_INC=4

# maximum number of client processes on each client machine
MAX_CLIENT_PROCESSES=128

# maximum coordinator scale factor, actual number of coordinators to feed each
# server will be (COOS_COARSE_UNIT * coor_scale), coor_scale will scale from
# 1, 2, 3, ..., COOS_COARSE_UNIT * MIN_COO_SCALE, COOS_COARSE_UNIT * (MIN_COO_SCALE + 1), ...,
# COOS_COARSE_UNIT * MAX_COO_SCALE
MAX_COO_SCALE=10
MIN_COO_SCALE=2

# coarse granularity to increment the number of coordinators
COOS_COARSE_UNIT=10
#COOS_COARSE_UNIT=2

# starting number of coordinators
COOS_START=1

# client machines
#CLIENT_MACHINES=( beaker-14 beaker-15 beaker-16 beaker-17 beaker-18 beaker-19 beaker-20 beaker-21 beaker-20 beaker-21 )
CLIENT_MACHINES=( beaker-14 beaker-15 beaker-17 beaker-18 beaker-19 beaker-20 beaker-21 beaker-20 beaker-21 )
# <<<<<<<< CLIENT OPTIONS <<<<<<<<


LOGGING_DIR="/scratch1/ycui"
DO_LOGGING=0


# >>>>>>>> BENCHMARK >>>>>>>>
# benchmarks to run
BENCHS=( tpcc_mix_10 tpcc_no_10 )
#BENCHS=( tpcc_mix_10 )

# interested txn name
#BENCH_INTEREST_TXN=( "NEW ORDER" "NEW ORDER" "NEW ORDER" "NEW ORDER" )
#BENCH_INTEREST_TXN=( "NEW ORDER" "NEW ORDER" )
BENCH_INTEREST_TXN=( "STOCK LEVEL" "STOCK LEVEL" )
#BENCH_INTEREST_TXN=( "NEW ORDER" )

# max retry
MAX_RETRY=5

# available modes
MODES=( deptran occ 2pl_wound_die ro6 )
#MODES=( deptran occ 2pl_wound_die )
#MODES=( 2pl )

# benchmark running duration
BENCH_DURATION=60

# TASKSET SCHEMA
TASKSET=1

# script ctrl port
CTRL_PORT=12000

# benchmark log
BENCHMARK_LOGGING_FILE="benchmark.log"
# <<<<<<<< BENCHMARK <<<<<<<<


# >>>>>>>> SCALE BENCH >>>>>>>>

SCALE_NUM_SERVS=( 1 2 3 4 5 10 20 30 40 50 60 70 80 90 100 )
# benchmarks to run
#SCALE_BENCHS=( tpcc_no_10 tpcc_mix_10 )
SCALE_BENCHS=( tpcc_mix_10 tpcc_no_10 )

# interested txn name
#SCALE_BENCH_INTEREST_TXN=( "NEW ORDER" "NEW ORDER" )
SCALE_BENCH_INTEREST_TXN=( "NEW ORDER" "NEW ORDER" )

# number of coordinators per server
SCALE_COOS_PER_SERV=( 10 20 40 )
#SCALE_COOS_PER_SERV=( 20 40 )

# minimum number of servers
SCALE_NUM_SERVS_START=1

# this parameter determines the maximum number of servers used for
# for scaling experiment, numbers of servers are:
# SCALE_NUM_SERVS_START * (2^0, 2^1, 2^2, ..., 2^SCALE_MAX_SERV_SCALE)
SCALE_MAX_SERV_SCALE=5

# <<<<<<<< SCALE BENCH <<<<<<<<



# >>>>>>>> MICRO BENCH >>>>>>>>

# null rpc starts from 1 to RPC_MAX_CLIENTS
NULL_RPC_MAX_CLIENTS=24

# kv starts from MICRO_BENCH_MIN_CLIENTS to MICRO_BENCH_MAX_CLIENTS
MICRO_BENCH_MIN_CLIENTS=8
MICRO_BENCH_MAX_CLIENTS=24

# for micro bench 4-piece txn
MICRO_BENCH_MAX_COOS=24
MICRO_BENCH_MIN_COOS=8

# for micro bench 4-piece txn
MICRO_BENCH_NUM_SERVS=4

MICRO_MODES=( none 2pl occ deptran )
# <<<<<<<< MICRO BENCH <<<<<<<<


GEN_CONFIG=0
DO_COMMON_BENCH=0
DO_SCALE_BENCH=0
DO_MICRO_BENCH=0

usage()
{
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo ""
    echo "    -h,"
    echo "            help"
    echo ""
    echo "    -g,"
    echo "            only generat configuration files, don't run benchmark"
    echo ""
    echo "    -p N,"
    echo "            set number of servers to run on each machine"
    echo ""
    echo "    -s N,"
    echo "            set total number of servers to run"
    echo ""
    echo "    -P port,"
    echo "            set server start port, for each server the port number will"
    echo "            be (start_port + server_id)"
    echo ""
    echo "    -S,"
    echo "            do scaling bench"
    echo ""
    echo "    -C,"
    echo "            do common benchmark"
    echo ""
    echo "    -M,"
    echo "            do micro benchmark"
    echo ""
}

while getopts "SCMhgp:s:P:" OPTION
do
    case ${OPTION} in
        g)
            GEN_CONFIG=1
            ;;
        h)
            usage
            exit 1
            ;;
        p)
            SERVS_PER_MACHINE=$OPTARG
            ;;
        P)
            SERV_START_PORT=$OPTARG
            ;;
        s)
            NUM_SERVS=$OPTARG
            ;;
        S)
            DO_SCALE_BENCH=1
            ;;
        C)
            DO_COMMON_BENCH=1
            ;;
        M)
            DO_MICRO_BENCH=1
            ;;
    esac
done

print_opt()
{
    echo ""
    echo "Parameters:"
    echo "  number of servers per machine   :   ${SERVS_PER_MACHINE}"
    echo "  total number of servers         :   ${NUM_SERVS}"
    echo "  server start port               :   ${SERV_START_PORT}"
    echo ""
}

print_opt

NL=$'\n'
INDENT="    "

# make server config string
# args: 1:  number of servers
make_server_config_str()
{
    local num_servers=${1}
    local server_line_temp="<site id=\"__SID__\" threads=\"1\">__S_MACHINE__:__S_PORT__</site>"
    local server_config_str="${INDENT}<hosts number=\"${num_servers}\">${NL}"
    local sid=0
    local s_m_id=0
    local s_port=${SERV_START_PORT}
    while [ ${sid} -lt ${num_servers} ]
    do
        server_one_line=`echo ${server_line_temp} | \
            sed "s/__SID__/${sid}/g" | \
            sed "s/__S_MACHINE__/${SERV_MACHINES[${s_m_id}]}/g" | \
            sed "s/__S_PORT__/${s_port}/g"`
        server_config_str="${server_config_str}${INDENT}${INDENT}${server_one_line}${NL}"
        sid=$((${sid}+1))
        if [ 0 -eq $((${sid}%${SERVS_PER_MACHINE})) ]
        then
            s_m_id=$((${s_m_id}+1))
            #s_port=${SERV_START_PORT}
            s_port=$((${s_port}+1))
        else
            s_port=$((${s_port}+1))
        fi
    done

    local server_config_str="${server_config_str}${INDENT}</hosts>"

    echo "${server_config_str}"
}

# make clients config string
# args: 1:  number of coordinators
make_client_config_str()
{
    local client_line_temp="<client id=\"__CID_START__-__CID_END__\" threads=\"${COOS_PER_CLIENT}\">__C_MACHINE__</client>"
    local client_line_temp_no_end="<client id=\"__CID_START__\" threads=\"${COOS_PER_CLIENT}\">__C_MACHINE__</client>"
    local num_coos=${1}
    if [ 0 -ne $((${num_coos}%${COOS_PER_CLIENT})) ]
    then
        echo "" >&2
        echo "Number of coordinators ${num_coos} not divided by number of" >&2
        echo "coordinators per client ${COOS_PER_CLIENT}" >&2
        echo "" >&2
        exit 3
    fi
    local num_clients=$((${num_coos}/${COOS_PER_CLIENT}))
    local client_config_str="${INDENT}<clients number=\"${num_clients}\">${NL}"

    local c_id_start=0
    local c_m_id=0
    while [ ${c_id_start} -lt ${num_clients} ]
    do
        c_id_end=$((${c_id_start}+${CLIENT_PROCESS_INC}-1))
        if [ ${c_id_end} -ge ${num_clients} ]
        then
            c_id_end=$((${num_clients}-1))
        fi
        if [ ${c_id_end} -eq ${c_id_start} ]
        then
            client_one_line=`echo ${client_line_temp_no_end} | \
                sed "s/__CID_START__/${c_id_start}/g" | \
                sed "s/__C_MACHINE__/${CLIENT_MACHINES[${c_m_id}]}/g"`
        else
            client_one_line=`echo ${client_line_temp} | \
                sed "s/__CID_START__/${c_id_start}/g" | \
                sed "s/__CID_END__/${c_id_end}/g" | \
                sed "s/__C_MACHINE__/${CLIENT_MACHINES[${c_m_id}]}/g"`
        fi
        client_config_str="${client_config_str}${INDENT}${INDENT}${client_one_line}${NL}"

        c_id_start=$((${c_id_end}+1))
        c_m_id=$((${c_m_id}+1))
        if [ ${c_m_id} -ge ${#CLIENT_MACHINES[@]} ]
        then
            c_m_id=0
        fi
    done

    local client_config_str="${client_config_str}${INDENT}</clients>"

    echo "${client_config_str}"
}

# make the whole configuration file
# args: 1:  benchmark template file
# args: 2:  mode
# args: 3:  number of servers
# args: 4:  number of coordinators
# args: 5:  scale factor
make_bench_config()
{
    local template_filename=${1}
    local mode=${2}
    local num_servers=${3}
    local num_coos=${4}
    local scale_factor=${5}
    local first_line=1
    while IFS='' read line
    do
        echo "${line}"
        if [ ${first_line} -eq 1 ]
        then
            make_server_config_str ${num_servers}
            make_client_config_str ${num_coos}
            first_line=0
        fi
    done < <(sed "s/__MODE__/${mode}/g" ${template_filename} | sed "s/__SCALE_FACTOR__/${scale_factor}/g")

}

do_scaling_bench()
{
    if [ ${#SCALE_BENCH_INTEREST_TXN[@]} -ne ${#SCALE_BENCHS[@]} ]
    then
        echo "" >&2
        echo "Numbers of scaling benchmark interested txns and benchmarks not consistent" >&2
        echo "" >&2
        exit 5
    fi

    local l_logging=""
    local l_f_name="nlog"
    if [ ${DO_LOGGING} -eq 1 ]
    then
        # logging
        l_logging="-r ${LOGGING_DIR}"
        l_f_name="log"
    fi

    local bench_index=0
    for bench_index in $(seq 0 $((${#SCALE_BENCHS[@]}-1)))
    do
        local num_coos=
        for num_coos in ${SCALE_COOS_PER_SERV[@]}
        do
            local bench="scale_${num_coos}coos_${SCALE_BENCHS[${bench_index}]}"
            local interested_txn=${SCALE_BENCH_INTEREST_TXN[${bench_index}]}
            local figure_gen_file=scripts/${bench}_${l_f_name}_figure_gen_local.py
            cp -f template/scale_figure_gen_template.py ${figure_gen_file}
            sed -i "s/__BENCH_NAME__/local\/${bench}_${l_f_name}/g" ${figure_gen_file}

            local template_file="./template/scale_${SCALE_BENCHS[${bench_index}]}.xml"

            #local num_servs=${SCALE_NUM_SERVS_START}
            local num_servs=1
            for num_servs in ${SCALE_NUM_SERVS[@]}
            do
                for mode in ${MODES[@]}
                do
                    # TRY TO FINISH A BENCH, TILL MAX_RETRY
                    local this_start_time=`date +"%H:%M:%S"`
                    local s_time_in_s=`date +"%s"`

                    local succ=0
                    local tried=0
                    while [ ${tried} -lt ${MAX_RETRY} ]
                    do
                        succ=0

                        # PREPARE TO DO BENCHMARK
                        local total_num_coos=$((${num_coos}*${num_servs}))
                        make_bench_config ${template_file} ${mode} ${num_servs} ${total_num_coos} ${num_servs} >tmp_conf.xml

                        # KILL UNFINISHED PROCESSES
                        for serv_m in ${SERV_MACHINES[@]}
                        do
                            ssh ${serv_m} 'killall -9 deptran_server 2>/dev/null'
                        done
                        for coor_m in ${CLIENT_MACHINES[@]}
                        do
                            ssh ${coor_m} 'killall -9 deptran_client 2>/dev/null'
                        done

                        # WAITING FOR CONFIG FLUSH TO TMP_CONF.XML
                        sleep 1

                        local l_txns=
                        local l_tries=
                        local l_commits=
                        local l_tps=
                        local l_latency_50=
                        local l_latency_90=
                        local l_latency_99=
                        local l_latency_999=
                        local l_att_lt_50=
                        local l_att_lt_90=
                        local l_att_lt_99=
                        local l_att_lt_999=
                        local l_c_time=
                        local l_min_latency=
                        local l_max_latency=

                        local l_graph=
                        local l_ask=
                        local l_scc=

                        local l_cpu_info="-1"
                        local l_log_flush_cnt="-1"
                        local l_log_flush_sz="-1"

                        # START BENCH AND GREP RESULT
                        while read line
                        do
                            echo "${line}"

                            if [[ ${line} == "BENCHMARK_SUCCEED" ]]
                            then
                                succ=1
                            fi

                            if [[ ${line} == *"RECORDING"* ]] && [[ ${line} == *"<${interested_txn}>"* ]]
                            then
                                # grep client statistics
                                space_out=`echo ${line} | sed "s/^.*FINISHED_TXNS: \([0-9]\+\); ATTEMPTS: \([0-9]\+\); COMMITS: \([0-9]\+\); TPS: \([0-9]\+\); 50\.0% LATENCY: \([0-9]*\.\?[0-9]*\); 90\.0% LATENCY: \([0-9]*\.\?[0-9]*\); 99\.0% LATENCY: \([0-9]*\.\?[0-9]*\); 99\.9% LATENCY: \([0-9]*\.\?[0-9]*\);.*/\1 \2 \3 \4 \5 \6 \7 \8/g"`
                                read l_txns l_tries l_commits l_tps l_latency_50 l_latency_90 l_latency_99 l_latency_999 <<< "${space_out}"
                                space_out=`echo ${line} | sed "s/^.*50\.0% ATT_LT: \([0-9]*\.\?[0-9]*\); 90\.0% ATT_LT: \([0-9]*\.\?[0-9]*\); 99\.0% ATT_LT: \([0-9]*\.\?[0-9]*\); 99\.9% ATT_LT: \([0-9]*\.\?[0-9]*\); TIME: \([0-9]*\.\?[0-9]*\); LATENCY MIN: \([0-9]*\.\?[0-9]*\); LATENCY MAX: \([0-9]*\.\?[0-9]*\);.*/\1 \2 \3 \4 \5 \6 \7/g"`
                                read l_att_lt_50 l_att_lt_90 l_att_lt_99 l_att_lt_999 l_c_time l_min_latency l_max_latency <<< "${space_out}"
                                #space_out=`echo ${line} | sed "s/^.*50\.0% N_TRY: \([0-9]*\.\?[0-9]*\); 90\.0% N_TRY: \([0-9]*\.\?[0-9]*\); 99\.0% N_TRY: \([0-9]*\.\?[0-9]*\); 99\.9% N_TRY: \([0-9]*\.\?[0-9]*\)/\1 \2 \3 \4/g"`
                                #read l_n_try_50 l_n_try_90 l_n_try_99 l_n_try_999 <<< "${space_out}"
                            elif [[ ${line} == *"SERVREC"* ]]
                            then
                                # grep server statistics
                                space_out=`echo ${line} | sed "s/^.*SERVREC: \([a-zA-Z]\+\): VALUE: \([0-9]\+\); TIMES: \([0-9]\+\); MEAN: \([0-9]*\.\?[0-9]*\); TIME: \([0-9]*\.\?[0-9]*\)/\1 \2 \3 \4 \5/g"`
                                read key value timez mean s_time <<< "${space_out}"
                                if [ ${key} == "graph" ]
                                then
                                    l_graph=${mean}
                                elif [ ${key} == "ask" ]
                                then
                                    l_ask="${value}\\/${s_time}"
                                elif [ ${key} == "scc" ]
                                then
                                    l_scc=${mean}
                                fi
                            elif [[ ${line} == *"CPUINFO"* ]]
                            then
                                l_cpu_info=`echo ${line} | sed "s/^.*CPUINFO: \([0-9]*\.\?[0-9]*\);/\1/g"`
                            elif [[ ${line} == *"AVG_LOG_FLUSH_CNT"* ]]
                            then
                                l_log_flush_cnt=`echo ${line} | sed "s/^.*AVG_LOG_FLUSH_CNT: \(-\?[0-9]*\.\?[0-9]*\);/\1/g"`
                            elif [[ ${line} == *"AVG_LOG_FLUSH_SZ"* ]]
                            then
                                l_log_flush_sz=`echo ${line} | sed "s/^.*AVG_LOG_FLUSH_SZ: \(-\?[0-9]*\.\?[0-9]*\);/\1/g"`
                            fi

                        done < <(./run.py -T ${TASKSET} -f tmp_conf.xml -d ${BENCH_DURATION} -P ${CTRL_PORT} ${l_logging} -x "${interested_txn}")

                        if [ ${succ} -eq 1 ] && \
                           [ ${#l_txns} -ne 0 ] && \
                           [ ${#l_tps} -ne 0 ] && \
                           [ ${#l_commits} -ne 0 ] && \
                           [ ${#l_tries} -ne 0 ] && \
                           [ ${#l_latency_50} -ne 0 ] && \
                           [ ${#l_latency_90} -ne 0 ] && \
                           [ ${#l_latency_99} -ne 0 ] && \
                           [ ${#l_latency_999} -ne 0 ] && \
                           [ ${#l_att_lt_50} -ne 0 ] && \
                           [ ${#l_att_lt_90} -ne 0 ] && \
                           [ ${#l_att_lt_99} -ne 0 ] && \
                           [ ${#l_att_lt_999} -ne 0 ] && \
                           [ ${#l_c_time} -ne 0 ] && \
                           [ ${#l_min_latency} -ne 0 ] && \
                           [ ${#l_max_latency} -ne 0 ]
                        then
                            # success
                            sed -i "s/__${mode}\.tps__/${l_tps}, __${mode}\.tps__/g" ${figure_gen_file}
                            if [ ${l_tries} -ne 0 ]
                            then
                                sed -i "s/__${mode}\.commit_rate__/${l_commits}\.0\/${l_tries}, __${mode}\.commit_rate__/g" ${figure_gen_file}
                            else
                                sed -i "s/__${mode}\.commit_rate__/0\.0, __${mode}\.commit_rate__/g" ${figure_gen_file}
                            fi
                            sed -i "s/__${mode}\.latency_50__/${l_latency_50}, __${mode}\.latency_50__/g" ${figure_gen_file}
                            sed -i "s/__${mode}\.latency_90__/${l_latency_90}, __${mode}\.latency_90__/g" ${figure_gen_file}
                            sed -i "s/__${mode}\.latency_99__/${l_latency_99}, __${mode}\.latency_99__/g" ${figure_gen_file}
                            sed -i "s/__${mode}\.latency_999__/${l_latency_999}, __${mode}\.latency_999__/g" ${figure_gen_file}
                            sed -i "s/__${mode}\.att_latency_50__/${l_att_lt_50}, __${mode}\.att_latency_50__/g" ${figure_gen_file}
                            sed -i "s/__${mode}\.att_latency_90__/${l_att_lt_90}, __${mode}\.att_latency_90__/g" ${figure_gen_file}
                            sed -i "s/__${mode}\.att_latency_99__/${l_att_lt_99}, __${mode}\.att_latency_99__/g" ${figure_gen_file}
                            sed -i "s/__${mode}\.att_latency_999__/${l_att_lt_999}, __${mode}\.att_latency_999__/g" ${figure_gen_file}
                            sed -i "s/__${mode}\.attempts__/${l_tries}\.0\/${l_c_time}, __${mode}\.attempts__/g" ${figure_gen_file}

                            if [ ${#l_graph} -ne 0 ]
                            then
                                sed -i "s/__${mode}\.graph_size__/${l_graph}, __${mode}\.graph_size__/g" ${figure_gen_file}
                            else
                                sed -i "s/__${mode}\.graph_size__/0\.0, __${mode}\.graph_size__/g" ${figure_gen_file}
                            fi
                            if [ ${#l_ask} -ne 0 ]
                            then
                                sed -i "s/__${mode}\.n_ask__/${l_ask}, __${mode}\.n_ask__/g" ${figure_gen_file}
                            else
                                sed -i "s/__${mode}\.n_ask__/0\.0, __${mode}\.n_ask__/g" ${figure_gen_file}
                            fi
                            if [ ${#l_scc} -ne 0 ]
                            then
                                sed -i "s/__${mode}\.scc__/${l_scc}, __${mode}\.scc__/g" ${figure_gen_file}
                            else
                                sed -i "s/__${mode}\.scc__/0\.0, __${mode}\.scc__/g" ${figure_gen_file}
                            fi
                            sed -i "s/__${mode}\.min_latency__/${l_min_latency}, __${mode}\.min_latency__/g" ${figure_gen_file}
                            sed -i "s/__${mode}\.max_latency__/${l_max_latency}, __${mode}\.max_latency__/g" ${figure_gen_file}
                            sed -i "s/__${mode}\.cpu__/${l_cpu_info}, __${mode}\.cpu__/g" ${figure_gen_file}
                            sed -i "s/__${mode}\.flush_cnt__/${l_log_flush_cnt}, __${mode}\.flush_cnt__/g" ${figure_gen_file}
                            sed -i "s/__${mode}\.flush_sz__/${l_log_flush_sz}, __${mode}\.flush_sz__/g" ${figure_gen_file}

                            tried=$((${tried}+1))
                            break
                        else
                            # fail
                            succ=0
                            echo "BENCH FAIL" >&2
                            echo "TRIED: $((1+${tried}))" >&2
                        fi

                        tried=$((${tried}+1))
                    done

                    local this_finish_time=`date +"%H:%M:%S"`
                    local f_time_in_s=`date +"%s"`
                    this_elapsed=$((${f_time_in_s}-${s_time_in_s}))
                    if [ ${succ} -ne 1 ]
                    then
                        # failed
                        echo "============== FAILURE ===============" >>${BENCHMARK_LOGGING_FILE}
                        echo "BENCH FAILED FOR ${MAX_RETRY} TIMES" >>${BENCHMARK_LOGGING_FILE}
                        echo "BENCHMARK     :   ${bench}" >>${BENCHMARK_LOGGING_FILE}
                        echo "INTERESTED TXN:   ${interested_txn}" >>${BENCHMARK_LOGGING_FILE}
                        echo "MODE          :   ${mode}" >>${BENCHMARK_LOGGING_FILE}
                        echo "NUM SERVS     :   ${num_servs}" >>${BENCHMARK_LOGGING_FILE}
                        echo "NUM COOS      :   ${num_coos}" >>${BENCHMARK_LOGGING_FILE}
                        echo "ELAPSED TIME  :   ${this_elapsed}" >>${BENCHMARK_LOGGING_FILE}
                        echo "START TIME    :   ${this_start_time}" >>${BENCHMARK_LOGGING_FILE}
                        echo "FINISH TIME   :   ${this_finish_time}" >>${BENCHMARK_LOGGING_FILE}
                        echo "============== FAILURE ===============" >>${BENCHMARK_LOGGING_FILE}
                        echo "" >>${BENCHMARK_LOGGING_FILE}

                        sed -i "s/__${mode}\.tps__/-1, __${mode}\.tps__/g" ${figure_gen_file}
                        sed -i "s/__${mode}\.commit_rate__/-1, __${mode}\.commit_rate__/g" ${figure_gen_file}
                        sed -i "s/__${mode}\.latency_50__/-1, __${mode}\.latency_50__/g" ${figure_gen_file}
                        sed -i "s/__${mode}\.latency_90__/-1, __${mode}\.latency_90__/g" ${figure_gen_file}
                        sed -i "s/__${mode}\.latency_99__/-1, __${mode}\.latency_99__/g" ${figure_gen_file}
                        sed -i "s/__${mode}\.latency_999__/-1, __${mode}\.latency_999__/g" ${figure_gen_file}
                        sed -i "s/__${mode}\.att_latency_50__/-1, __${mode}\.att_latency_50__/g" ${figure_gen_file}
                        sed -i "s/__${mode}\.att_latency_90__/-1, __${mode}\.att_latency_90__/g" ${figure_gen_file}
                        sed -i "s/__${mode}\.att_latency_99__/-1, __${mode}\.att_latency_99__/g" ${figure_gen_file}
                        sed -i "s/__${mode}\.att_latency_999__/-1, __${mode}\.att_latency_999__/g" ${figure_gen_file}
                        sed -i "s/__${mode}\.attempts__/-1, __${mode}\.attempts__/g" ${figure_gen_file}

                        sed -i "s/__${mode}\.graph_size__/-1, __${mode}\.graph_size__/g" ${figure_gen_file}
                        sed -i "s/__${mode}\.n_ask__/-1, __${mode}\.n_ask__/g" ${figure_gen_file}
                        sed -i "s/__${mode}\.scc__/-1, __${mode}\.scc__/g" ${figure_gen_file}
                        sed -i "s/__${mode}\.min_latency__/-1, __${mode}\.min_latency__/g" ${figure_gen_file}
                        sed -i "s/__${mode}\.max_latency__/-1, __${mode}\.max_latency__/g" ${figure_gen_file}
                        sed -i "s/__${mode}\.cpu__/-1, __${mode}\.cpu__/g" ${figure_gen_file}
                        sed -i "s/__${mode}\.flush_cnt__/-1, __${mode}\.flush_cnt__/g" ${figure_gen_file}
                        sed -i "s/__${mode}\.flush_sz__/-1, __${mode}\.flush_sz__/g" ${figure_gen_file}

                        # backup xml file
                        mkdir -p failed_xml/${bench}
                        cp -f tmp_conf.xml failed_xml/${bench}/${mode}_${num_serv}servs_${num_coos}coos.xml
                    else
                        echo "============== SUCCESS ===============" >>${BENCHMARK_LOGGING_FILE}
                        echo "TRIED         :   ${tried}" >>${BENCHMARK_LOGGING_FILE}
                        echo "BENCHMARK     :   ${bench}" >>${BENCHMARK_LOGGING_FILE}
                        echo "INTERESTED TXN:   ${interested_txn}" >>${BENCHMARK_LOGGING_FILE}
                        echo "MODE          :   ${mode}" >>${BENCHMARK_LOGGING_FILE}
                        echo "NUM SERVS     :   ${num_servs}" >>${BENCHMARK_LOGGING_FILE}
                        echo "NUM COOS      :   ${num_coos}" >>${BENCHMARK_LOGGING_FILE}
                        echo "ELAPSED TIME  :   ${this_elapsed}" >>${BENCHMARK_LOGGING_FILE}
                        echo "START TIME    :   ${this_start_time}" >>${BENCHMARK_LOGGING_FILE}
                        echo "FINISH TIME   :   ${this_finish_time}" >>${BENCHMARK_LOGGING_FILE}
                        echo "============== SUCCESS ===============" >>${BENCHMARK_LOGGING_FILE}
                        echo "" >>${BENCHMARK_LOGGING_FILE}
                    fi
                done

                # add x axis
                sed -i "s/__num_servs__/${num_servs}, __num_servs__/g" ${figure_gen_file}

                #num_servs=$((${num_servs}*2))
            done

            sed -i "s/, __num_servs__//g" ${figure_gen_file}

            for mode in ${MODES[@]}
            do
                sed -i "s/, __${mode}\.tps__//g" ${figure_gen_file}
                sed -i "s/, __${mode}\.commit_rate__//g" ${figure_gen_file}
                sed -i "s/, __${mode}\.latency_50__//g" ${figure_gen_file}
                sed -i "s/, __${mode}\.latency_90__//g" ${figure_gen_file}
                sed -i "s/, __${mode}\.latency_99__//g" ${figure_gen_file}
                sed -i "s/, __${mode}\.latency_999__//g" ${figure_gen_file}
                sed -i "s/, __${mode}\.att_latency_50__//g" ${figure_gen_file}
                sed -i "s/, __${mode}\.att_latency_90__//g" ${figure_gen_file}
                sed -i "s/, __${mode}\.att_latency_99__//g" ${figure_gen_file}
                sed -i "s/, __${mode}\.att_latency_999__//g" ${figure_gen_file}
                sed -i "s/, __${mode}\.attempts__//g" ${figure_gen_file}
                sed -i "s/, __${mode}\.graph_size__//g" ${figure_gen_file}
                sed -i "s/, __${mode}\.n_ask__//g" ${figure_gen_file}
                sed -i "s/, __${mode}\.scc__//g" ${figure_gen_file}
                sed -i "s/, __${mode}\.min_latency__//g" ${figure_gen_file}
                sed -i "s/, __${mode}\.max_latency__//g" ${figure_gen_file}
                sed -i "s/, __${mode}\.cpu__//g" ${figure_gen_file}
                sed -i "s/, __${mode}\.flush_cnt__//g" ${figure_gen_file}
                sed -i "s/, __${mode}\.flush_sz__//g" ${figure_gen_file}
            done

            rm -f tmp_conf.xml 2>/dev/null
        done
    done
}

do_common_bench()
{
    if [ ${#BENCH_INTEREST_TXN[@]} -ne ${#BENCHS[@]} ]
    then
        echo "" >&2
        echo "Numbers of benchmark interested txns and benchmarks not consistent" >&2
        echo "" >&2
        exit 5
    fi

    local l_logging=""
    local l_f_name="nlog"
    if [ ${DO_LOGGING} -eq 1 ]
    then
        # logging
        l_logging="-r ${LOGGING_DIR}"
        l_f_name="log"
    fi

    local bench_index=0
    for bench_index in $(seq 0 $((${#BENCHS[@]}-1)))
    do
        local bench=${BENCHS[${bench_index}]}
        local interested_txn=${BENCH_INTEREST_TXN[${bench_index}]}
        local figure_gen_file=scripts/${bench}_${l_f_name}_figure_gen_local.py
        cp -f template/figure_gen_template.py ${figure_gen_file}
        sed -i "s/__BENCH_NAME__/local\/${bench}_${l_f_name}/g" ${figure_gen_file}

        local template_file="./template/${bench}.xml"

        local coo_scale=${MIN_COO_SCALE}
        local num_coos=${COOS_START}
        while [ ${coo_scale} -le ${MAX_COO_SCALE} ]
        do
            for mode in ${MODES[@]}
            do
                # TRY TO FINISH A BENCH, TILL MAX_RETRY
                local this_start_time=`date +"%H:%M:%S"`
                local s_time_in_s=`date +"%s"`

                local succ=0
                local tried=0
                while [ ${tried} -lt ${MAX_RETRY} ]
                do
                    succ=0

                    # PREPARE TO DO BENCHMARK
                    local total_num_coos=$((${num_coos}*${NUM_SERVS}))
                    make_bench_config ${template_file} ${mode} ${NUM_SERVS} ${total_num_coos} ${NUM_SERVS} >tmp_conf.xml

                    # KILL UNFINISHED PROCESSES
                    for serv_m in ${SERV_MACHINES[@]}
                    do
                        ssh ${serv_m} 'killall -9 deptran_server 2>/dev/null'
                    done
                    for coor_m in ${CLIENT_MACHINES[@]}
                    do
                        ssh ${coor_m} 'killall -9 deptran_client 2>/dev/null'
                    done

                    # WAITING FOR CONFIG FLUSH TO TMP_CONF.XML
                    sleep 1

                    local l_txns=
                    local l_tries=
                    local l_commits=
                    local l_tps=
                    local l_latency_50=
                    local l_latency_90=
                    local l_latency_99=
                    local l_latency_999=
                    local l_att_lt_50=
                    local l_att_lt_90=
                    local l_att_lt_99=
                    local l_att_lt_999=
                    local l_c_time=
                    local l_min_latency=
                    local l_max_latency=
                    local l_n_try_50=
                    local l_n_try_90=
                    local l_n_try_99=
                    local l_n_try_999=

                    local l_graph=
                    local l_ask=
                    local l_scc=

                    local l_cpu_info="-1"
                    local l_log_flush_cnt="-1"
                    local l_log_flush_sz="-1"

                    # START BENCH AND GREP RESULT
                    while read line
                    do
                        echo "${line}"

                        if [[ ${line} == "BENCHMARK_SUCCEED" ]]
                        then
                            succ=1
                        fi

                        if [[ ${line} == *"RECORDING"* ]] && [[ ${line} == *"<${interested_txn}>"* ]]
                        then
                            # grep client statistics
                            space_out=`echo ${line} | sed "s/^.*FINISHED_TXNS: \([0-9]\+\); ATTEMPTS: \([0-9]\+\); COMMITS: \([0-9]\+\); TPS: \([0-9]\+\); 50\.0% LATENCY: \([0-9]*\.\?[0-9]*\); 90\.0% LATENCY: \([0-9]*\.\?[0-9]*\); 99\.0% LATENCY: \([0-9]*\.\?[0-9]*\); 99\.9% LATENCY: \([0-9]*\.\?[0-9]*\);.*/\1 \2 \3 \4 \5 \6 \7 \8/g"`
                            read l_txns l_tries l_commits l_tps l_latency_50 l_latency_90 l_latency_99 l_latency_999 <<< "${space_out}"
                            space_out=`echo ${line} | sed "s/^.*50\.0% ATT_LT: \([0-9]*\.\?[0-9]*\); 90\.0% ATT_LT: \([0-9]*\.\?[0-9]*\); 99\.0% ATT_LT: \([0-9]*\.\?[0-9]*\); 99\.9% ATT_LT: \([0-9]*\.\?[0-9]*\); TIME: \([0-9]*\.\?[0-9]*\); LATENCY MIN: \([0-9]*\.\?[0-9]*\); LATENCY MAX: \([0-9]*\.\?[0-9]*\);.*/\1 \2 \3 \4 \5 \6 \7/g"`
                            read l_att_lt_50 l_att_lt_90 l_att_lt_99 l_att_lt_999 l_c_time l_min_latency l_max_latency <<< "${space_out}"
                            space_out=`echo ${line} | sed "s/^.*50\.0% N_TRY: \([0-9]*\.\?[0-9]*\); 90\.0% N_TRY: \([0-9]*\.\?[0-9]*\); 99\.0% N_TRY: \([0-9]*\.\?[0-9]*\); 99\.9% N_TRY: \([0-9]*\.\?[0-9]*\)/\1 \2 \3 \4/g"`
                            read l_n_try_50 l_n_try_90 l_n_try_99 l_n_try_999 <<< "${space_out}"
                        elif [[ ${line} == *"SERVREC"* ]]
                        then
                            # grep server statistics
                            space_out=`echo ${line} | sed "s/^.*SERVREC: \([a-zA-Z]\+\): VALUE: \([0-9]\+\); TIMES: \([0-9]\+\); MEAN: \([0-9]*\.\?[0-9]*\); TIME: \([0-9]*\.\?[0-9]*\)/\1 \2 \3 \4 \5/g"`
                            read key value timez mean s_time <<< "${space_out}"
                            if [ ${key} == "graph" ]
                            then
                                l_graph=${mean}
                            elif [ ${key} == "ask" ]
                            then
                                l_ask="${value}\\/${s_time}"
                            elif [ ${key} == "scc" ]
                            then
                                l_scc=${mean}
                            fi
                        elif [[ ${line} == *"CPUINFO"* ]]
                        then
                            l_cpu_info=`echo ${line} | sed "s/^.*CPUINFO: \([0-9]*\.\?[0-9]*\);/\1/g"`
                        elif [[ ${line} == *"AVG_LOG_FLUSH_CNT"* ]]
                        then
                            l_log_flush_cnt=`echo ${line} | sed "s/^.*AVG_LOG_FLUSH_CNT: \(-\?[0-9]*\.\?[0-9]*\);/\1/g"`
                        elif [[ ${line} == *"AVG_LOG_FLUSH_SZ"* ]]
                        then
                            l_log_flush_sz=`echo ${line} | sed "s/^.*AVG_LOG_FLUSH_SZ: \(-\?[0-9]*\.\?[0-9]*\);/\1/g"`
                        fi

                    done < <(./run.py -T ${TASKSET} -f tmp_conf.xml -d ${BENCH_DURATION} -P ${CTRL_PORT} ${l_logging} -x "${interested_txn}")

                    if [ ${succ} -eq 1 ] && \
                       [ ${#l_txns} -ne 0 ] && \
                       [ ${#l_tps} -ne 0 ] && \
                       [ ${#l_commits} -ne 0 ] && \
                       [ ${#l_tries} -ne 0 ] && \
                       [ ${#l_latency_50} -ne 0 ] && \
                       [ ${#l_latency_90} -ne 0 ] && \
                       [ ${#l_latency_99} -ne 0 ] && \
                       [ ${#l_latency_999} -ne 0 ] && \
                       [ ${#l_n_try_50} -ne 0 ] && \
                       [ ${#l_n_try_90} -ne 0 ] && \
                       [ ${#l_n_try_99} -ne 0 ] && \
                       [ ${#l_n_try_999} -ne 0 ] && \
                       [ ${#l_c_time} -ne 0 ] && \
                       [ ${#l_min_latency} -ne 0 ] && \
                       [ ${#l_max_latency} -ne 0 ]
                    then
                        # success
                        sed -i "s/__${mode}\.tps__/${l_tps}, __${mode}\.tps__/g" ${figure_gen_file}
                        if [ ${l_tries} -ne 0 ]
                        then
                            sed -i "s/__${mode}\.commit_rate__/${l_commits}\.0\/${l_tries}, __${mode}\.commit_rate__/g" ${figure_gen_file}
                        else
                            sed -i "s/__${mode}\.commit_rate__/0\.0, __${mode}\.commit_rate__/g" ${figure_gen_file}
                        fi
                        sed -i "s/__${mode}\.latency_50__/${l_latency_50}, __${mode}\.latency_50__/g" ${figure_gen_file}
                        sed -i "s/__${mode}\.latency_90__/${l_latency_90}, __${mode}\.latency_90__/g" ${figure_gen_file}
                        sed -i "s/__${mode}\.latency_99__/${l_latency_99}, __${mode}\.latency_99__/g" ${figure_gen_file}
                        sed -i "s/__${mode}\.latency_999__/${l_latency_999}, __${mode}\.latency_999__/g" ${figure_gen_file}
                        sed -i "s/__${mode}\.n_try_50__/${l_n_try_50}, __${mode}\.n_try_50__/g" ${figure_gen_file}
                        sed -i "s/__${mode}\.n_try_90__/${l_n_try_90}, __${mode}\.n_try_90__/g" ${figure_gen_file}
                        sed -i "s/__${mode}\.n_try_99__/${l_n_try_99}, __${mode}\.n_try_99__/g" ${figure_gen_file}
                        sed -i "s/__${mode}\.n_try_999__/${l_n_try_999}, __${mode}\.n_try_999__/g" ${figure_gen_file}
                        sed -i "s/__${mode}\.att_latency_50__/${l_att_lt_50}, __${mode}\.att_latency_50__/g" ${figure_gen_file}
                        sed -i "s/__${mode}\.att_latency_90__/${l_att_lt_90}, __${mode}\.att_latency_90__/g" ${figure_gen_file}
                        sed -i "s/__${mode}\.att_latency_99__/${l_att_lt_99}, __${mode}\.att_latency_99__/g" ${figure_gen_file}
                        sed -i "s/__${mode}\.att_latency_999__/${l_att_lt_999}, __${mode}\.att_latency_999__/g" ${figure_gen_file}
                        sed -i "s/__${mode}\.attempts__/${l_tries}\.0\/${l_c_time}, __${mode}\.attempts__/g" ${figure_gen_file}

                        if [ ${#l_graph} -ne 0 ]
                        then
                            sed -i "s/__${mode}\.graph_size__/${l_graph}, __${mode}\.graph_size__/g" ${figure_gen_file}
                        else
                            sed -i "s/__${mode}\.graph_size__/0\.0, __${mode}\.graph_size__/g" ${figure_gen_file}
                        fi
                        if [ ${#l_ask} -ne 0 ]
                        then
                            sed -i "s/__${mode}\.n_ask__/${l_ask}, __${mode}\.n_ask__/g" ${figure_gen_file}
                        else
                            sed -i "s/__${mode}\.n_ask__/0\.0, __${mode}\.n_ask__/g" ${figure_gen_file}
                        fi
                        if [ ${#l_scc} -ne 0 ]
                        then
                            sed -i "s/__${mode}\.scc__/${l_scc}, __${mode}\.scc__/g" ${figure_gen_file}
                        else
                            sed -i "s/__${mode}\.scc__/0\.0, __${mode}\.scc__/g" ${figure_gen_file}
                        fi
                        sed -i "s/__${mode}\.min_latency__/${l_min_latency}, __${mode}\.min_latency__/g" ${figure_gen_file}
                        sed -i "s/__${mode}\.max_latency__/${l_max_latency}, __${mode}\.max_latency__/g" ${figure_gen_file}
                        sed -i "s/__${mode}\.cpu__/${l_cpu_info}, __${mode}\.cpu__/g" ${figure_gen_file}
                        sed -i "s/__${mode}\.flush_cnt__/${l_log_flush_cnt}, __${mode}\.flush_cnt__/g" ${figure_gen_file}
                        sed -i "s/__${mode}\.flush_sz__/${l_log_flush_sz}, __${mode}\.flush_sz__/g" ${figure_gen_file}

                        tried=$((${tried}+1))
                        break
                    else
                        # fail
                        succ=0
                        echo "BENCH FAIL" >&2
                        echo "TRIED: $((1+${tried}))" >&2
                    fi

                    tried=$((${tried}+1))
                done

                local this_finish_time=`date +"%H:%M:%S"`
                local f_time_in_s=`date +"%s"`
                this_elapsed=$((${f_time_in_s}-${s_time_in_s}))
                if [ ${succ} -ne 1 ]
                then
                    # failed
                    echo "============== FAILURE ===============" >>${BENCHMARK_LOGGING_FILE}
                    echo "BENCH FAILED FOR ${MAX_RETRY} TIMES" >>${BENCHMARK_LOGGING_FILE}
                    echo "BENCHMARK     :   ${bench}" >>${BENCHMARK_LOGGING_FILE}
                    echo "INTERESTED TXN:   ${interested_txn}" >>${BENCHMARK_LOGGING_FILE}
                    echo "MODE          :   ${mode}" >>${BENCHMARK_LOGGING_FILE}
                    echo "NUM SERVS     :   ${NUM_SERVS}" >>${BENCHMARK_LOGGING_FILE}
                    echo "NUM COOS      :   ${num_coos}" >>${BENCHMARK_LOGGING_FILE}
                    echo "ELAPSED TIME  :   ${this_elapsed}" >>${BENCHMARK_LOGGING_FILE}
                    echo "START TIME    :   ${this_start_time}" >>${BENCHMARK_LOGGING_FILE}
                    echo "FINISH TIME   :   ${this_finish_time}" >>${BENCHMARK_LOGGING_FILE}
                    echo "============== FAILURE ===============" >>${BENCHMARK_LOGGING_FILE}
                    echo "" >>${BENCHMARK_LOGGING_FILE}

                    sed -i "s/__${mode}\.tps__/-1, __${mode}\.tps__/g" ${figure_gen_file}
                    sed -i "s/__${mode}\.commit_rate__/-1, __${mode}\.commit_rate__/g" ${figure_gen_file}
                    sed -i "s/__${mode}\.latency_50__/-1, __${mode}\.latency_50__/g" ${figure_gen_file}
                    sed -i "s/__${mode}\.latency_90__/-1, __${mode}\.latency_90__/g" ${figure_gen_file}
                    sed -i "s/__${mode}\.latency_99__/-1, __${mode}\.latency_99__/g" ${figure_gen_file}
                    sed -i "s/__${mode}\.latency_999__/-1, __${mode}\.latency_999__/g" ${figure_gen_file}
                    sed -i "s/__${mode}\.n_try_50__/-1, __${mode}\.n_try_50__/g" ${figure_gen_file}
                    sed -i "s/__${mode}\.n_try_90__/-1, __${mode}\.n_try_90__/g" ${figure_gen_file}
                    sed -i "s/__${mode}\.n_try_99__/-1, __${mode}\.n_try_99__/g" ${figure_gen_file}
                    sed -i "s/__${mode}\.n_try_999__/-1, __${mode}\.n_try_999__/g" ${figure_gen_file}
                    sed -i "s/__${mode}\.att_latency_50__/-1, __${mode}\.att_latency_50__/g" ${figure_gen_file}
                    sed -i "s/__${mode}\.att_latency_90__/-1, __${mode}\.att_latency_90__/g" ${figure_gen_file}
                    sed -i "s/__${mode}\.att_latency_99__/-1, __${mode}\.att_latency_99__/g" ${figure_gen_file}
                    sed -i "s/__${mode}\.att_latency_999__/-1, __${mode}\.att_latency_999__/g" ${figure_gen_file}
                    sed -i "s/__${mode}\.attempts__/-1, __${mode}\.attempts__/g" ${figure_gen_file}

                    sed -i "s/__${mode}\.graph_size__/-1, __${mode}\.graph_size__/g" ${figure_gen_file}
                    sed -i "s/__${mode}\.n_ask__/-1, __${mode}\.n_ask__/g" ${figure_gen_file}
                    sed -i "s/__${mode}\.scc__/-1, __${mode}\.scc__/g" ${figure_gen_file}
                    sed -i "s/__${mode}\.min_latency__/-1, __${mode}\.min_latency__/g" ${figure_gen_file}
                    sed -i "s/__${mode}\.max_latency__/-1, __${mode}\.max_latency__/g" ${figure_gen_file}
                    sed -i "s/__${mode}\.cpu__/-1, __${mode}\.cpu__/g" ${figure_gen_file}
                    sed -i "s/__${mode}\.flush_cnt__/-1, __${mode}\.flush_cnt__/g" ${figure_gen_file}
                    sed -i "s/__${mode}\.flush_sz__/-1, __${mode}\.flush_sz__/g" ${figure_gen_file}

                    # backup xml file
                    mkdir -p failed_xml/${bench}
                    cp -f tmp_conf.xml failed_xml/${bench}/${mode}_${num_coos}coos.xml
                else
                    echo "============== SUCCESS ===============" >>${BENCHMARK_LOGGING_FILE}
                    echo "TRIED         :   ${tried}" >>${BENCHMARK_LOGGING_FILE}
                    echo "BENCHMARK     :   ${bench}" >>${BENCHMARK_LOGGING_FILE}
                    echo "INTERESTED TXN:   ${interested_txn}" >>${BENCHMARK_LOGGING_FILE}
                    echo "MODE          :   ${mode}" >>${BENCHMARK_LOGGING_FILE}
                    echo "NUM SERVS     :   ${NUM_SERVS}" >>${BENCHMARK_LOGGING_FILE}
                    echo "NUM COOS      :   ${num_coos}" >>${BENCHMARK_LOGGING_FILE}
                    echo "ELAPSED TIME  :   ${this_elapsed}" >>${BENCHMARK_LOGGING_FILE}
                    echo "START TIME    :   ${this_start_time}" >>${BENCHMARK_LOGGING_FILE}
                    echo "FINISH TIME   :   ${this_finish_time}" >>${BENCHMARK_LOGGING_FILE}
                    echo "============== SUCCESS ===============" >>${BENCHMARK_LOGGING_FILE}
                    echo "" >>${BENCHMARK_LOGGING_FILE}
                fi
            done

            # add x axis
            sed -i "s/__num_coos__/${num_coos}, __num_coos__/g" ${figure_gen_file}

            if [ ${num_coos} -lt $((${COOS_COARSE_UNIT}*${MIN_COO_SCALE})) ]
            then
                num_coos=$((${num_coos}+1))
            else
                coo_scale=$((${coo_scale}+1))
                num_coos=$((${COOS_COARSE_UNIT}*${coo_scale}))
            fi
        done

        sed -i "s/, __num_coos__//g" ${figure_gen_file}

        for mode in ${MODES[@]}
        do
            sed -i "s/, __${mode}\.tps__//g" ${figure_gen_file}
            sed -i "s/, __${mode}\.commit_rate__//g" ${figure_gen_file}
            sed -i "s/, __${mode}\.latency_50__//g" ${figure_gen_file}
            sed -i "s/, __${mode}\.latency_90__//g" ${figure_gen_file}
            sed -i "s/, __${mode}\.latency_99__//g" ${figure_gen_file}
            sed -i "s/, __${mode}\.latency_999__//g" ${figure_gen_file}
            sed -i "s/, __${mode}\.n_try_50__//g" ${figure_gen_file}
            sed -i "s/, __${mode}\.n_try_90__//g" ${figure_gen_file}
            sed -i "s/, __${mode}\.n_try_99__//g" ${figure_gen_file}
            sed -i "s/, __${mode}\.n_try_999__//g" ${figure_gen_file}
            sed -i "s/, __${mode}\.att_latency_50__//g" ${figure_gen_file}
            sed -i "s/, __${mode}\.att_latency_90__//g" ${figure_gen_file}
            sed -i "s/, __${mode}\.att_latency_99__//g" ${figure_gen_file}
            sed -i "s/, __${mode}\.att_latency_999__//g" ${figure_gen_file}
            sed -i "s/, __${mode}\.attempts__//g" ${figure_gen_file}
            sed -i "s/, __${mode}\.graph_size__//g" ${figure_gen_file}
            sed -i "s/, __${mode}\.n_ask__//g" ${figure_gen_file}
            sed -i "s/, __${mode}\.scc__//g" ${figure_gen_file}
            sed -i "s/, __${mode}\.min_latency__//g" ${figure_gen_file}
            sed -i "s/, __${mode}\.max_latency__//g" ${figure_gen_file}
            sed -i "s/, __${mode}\.cpu__//g" ${figure_gen_file}
            sed -i "s/, __${mode}\.flush_cnt__//g" ${figure_gen_file}
            sed -i "s/, __${mode}\.flush_sz__//g" ${figure_gen_file}
        done

        rm -f tmp_conf.xml 2>/dev/null
    done
}

do_null_rpc_bench()
{
    local figure_gen_file=$1
    local num_c=0
    for num_c in $(seq 1 ${NULL_RPC_MAX_CLIENTS})
    do
        local l_qps=0
        local l_50_lt=0
        local l_90_lt=0
        local l_99_lt=0
        local l_999_lt=0

        local succ=0
        local tried=0
        while [ ${tried} -lt ${MAX_RETRY} ]
        do
            ssh -nf ${SERV_MACHINES[0]} "cd ${FULL_PATH} && taskset -c 16 ./build/rpc_microbench -s 0.0.0.0:${SERV_START_PORT} -b 0 -w 1 -e 1 1>/dev/null 2>/dev/null &"

            sleep 1
            while read line
            do
                echo "${line}"
                if [[ ${line} == *"QPS:"* ]]
                then
                    local output=`echo ${line} | sed "s/QPS: \([0-9]*\); 50.0% LATENCY: \([0-9]*\.\?[0-9]*\); 90.0% LATENCY: \([0-9]*\.\?[0-9]*\); 99.0% LATENCY: \([0-9]*\.\?[0-9]*\); 99.9% LATENCY: \([0-9]*\.\?[0-9]*\)/\1 \2 \3 \4 \5/g"`
                    read l_qps l_50_lt l_90_lt l_99_lt l_999_lt <<< "${output}"
                    succ=1
                fi
            done < <(ssh ${CLIENT_MACHINES[0]} "cd ${FULL_PATH} && ./build/rpc_microbench -f -c ${SERV_MACHINES[0]}:${SERV_START_PORT} -t ${num_c} -w 1 -n 20 -b 0 -o 1")
            ssh ${SERV_MACHINES[0]} 'killall -9 rpc_microbench &>/dev/null'
            tried=$((${tried}+1))

            if [ ${succ} -eq 1 ]
            then
                break
            fi
        done

        if [ ${succ} -eq 1 ]
        then
            sed -i "s/__null_rpc\.qps__/${l_qps}, __null_rpc\.qps__/g" ${figure_gen_file}
            sed -i "s/__null_rpc\.latency_50__/${l_50_lt}/g" ${figure_gen_file}
            sed -i "s/__null_rpc\.latency_90__/${l_90_lt}/g" ${figure_gen_file}
            sed -i "s/__null_rpc\.latency_99__/${l_99_lt}/g" ${figure_gen_file}
            sed -i "s/__null_rpc\.latency_999__/${l_999_lt}/g" ${figure_gen_file}

            echo "============== SUCCESS ===============" >>${BENCHMARK_LOGGING_FILE}
            echo "TRIED         :   ${tried}" >>${BENCHMARK_LOGGING_FILE}
            echo "BENCHMARK     :   micro_bench (null rpc)" >>${BENCHMARK_LOGGING_FILE}
            echo "NUM CLIENTS   :   ${num_c}" >>${BENCHMARK_LOGGING_FILE}
            echo "============== SUCCESS ===============" >>${BENCHMARK_LOGGING_FILE}
            echo "" >>${BENCHMARK_LOGGING_FILE}
        else
            sed -i "s/__null_rpc\.qps__/-1, __null_rpc\.qps__/g" ${figure_gen_file}
            sed -i "s/__null_rpc\.latency_50__/-1/g" ${figure_gen_file}
            sed -i "s/__null_rpc\.latency_90__/-1/g" ${figure_gen_file}
            sed -i "s/__null_rpc\.latency_99__/-1/g" ${figure_gen_file}
            sed -i "s/__null_rpc\.latency_999__/-1/g" ${figure_gen_file}

            echo "============== FAILURE ===============" >>${BENCHMARK_LOGGING_FILE}
            echo "TRIED         :   ${tried}" >>${BENCHMARK_LOGGING_FILE}
            echo "BENCHMARK     :   micro_bench (null rpc)" >>${BENCHMARK_LOGGING_FILE}
            echo "NUM CLIENTS   :   ${num_c}" >>${BENCHMARK_LOGGING_FILE}
            echo "============== FAILURE ===============" >>${BENCHMARK_LOGGING_FILE}
            echo "" >>${BENCHMARK_LOGGING_FILE}
        fi

        sed -i "s/__null_rpc\.num_coos__/${num_c}, __null_rpc\.num_coos__/g" ${figure_gen_file}
    done

    sed -i "s/, __null_rpc\.qps__//g" ${figure_gen_file}
    sed -i "s/, __null_rpc\.num_coos__//g" ${figure_gen_file}
}

do_kv_bench()
{
    local figure_gen_file=$1

    local template_file="./template/kv_bench.xml"

    local num_coos=1
    local mode="none"

    local interested_txn="WRITE"
    # TRY TO FINISH A BENCH, TILL MAX_RETRY
    local this_start_time=`date +"%H:%M:%S"`
    local s_time_in_s=`date +"%s"`

    local succ=0
    local tried=0
    while [ ${tried} -lt ${MAX_RETRY} ]
    do
        succ=0

        # PREPARE TO DO BENCHMARK
        make_bench_config ${template_file} ${mode} 1 ${num_coos} 1 >tmp_conf.xml

        # KILL UNFINISHED PROCESSES
        for serv_m in ${SERV_MACHINES[@]}
        do
            ssh ${serv_m} 'killall -9 deptran_server 2>/dev/null'
        done
        for coor_m in ${CLIENT_MACHINES[@]}
        do
            ssh ${coor_m} 'killall -9 deptran_client 2>/dev/null'
        done

        # WAITING FOR CONFIG FLUSH TO TMP_CONF.XML
        sleep 1

        local l_txns=
        local l_tries=
        local l_commits=
        local l_tps=
        local l_latency_50=
        local l_latency_90=
        local l_latency_99=
        local l_latency_999=
        local l_att_lt_50=
        local l_att_lt_90=
        local l_att_lt_99=
        local l_att_lt_999=
        local l_c_time=
        local l_min_latency=
        local l_max_latency=

        # START BENCH AND GREP RESULT
        while read line
        do
            echo "${line}"

            if [[ ${line} == "BENCHMARK_SUCCEED" ]]
            then
                succ=1
            fi

            if [[ ${line} == *"RECORDING"* ]] && [[ ${line} == *"<${interested_txn}>"* ]]
            then
                # grep client statistics
                space_out=`echo ${line} | sed "s/^.*FINISHED_TXNS: \([0-9]\+\); ATTEMPTS: \([0-9]\+\); COMMITS: \([0-9]\+\); TPS: \([0-9]\+\); 50\.0% LATENCY: \([0-9]*\.\?[0-9]*\); 90\.0% LATENCY: \([0-9]*\.\?[0-9]*\); 99\.0% LATENCY: \([0-9]*\.\?[0-9]*\); 99\.9% LATENCY: \([0-9]*\.\?[0-9]*\);.*/\1 \2 \3 \4 \5 \6 \7 \8/g"`
                read l_txns l_tries l_commits l_tps l_latency_50 l_latency_90 l_latency_99 l_latency_999 <<< "${space_out}"
                space_out=`echo ${line} | sed "s/^.*50\.0% ATT_LT: \([0-9]*\.\?[0-9]*\); 90\.0% ATT_LT: \([0-9]*\.\?[0-9]*\); 99\.0% ATT_LT: \([0-9]*\.\?[0-9]*\); 99\.9% ATT_LT: \([0-9]*\.\?[0-9]*\); TIME: \([0-9]*\.\?[0-9]*\); LATENCY MIN: \([0-9]*\.\?[0-9]*\); LATENCY MAX: \([0-9]*\.\?[0-9]*\);.*/\1 \2 \3 \4 \5 \6 \7/g"`
                read l_att_lt_50 l_att_lt_90 l_att_lt_99 l_att_lt_999 l_c_time l_min_latency l_max_latency <<< "${space_out}"
            fi

        done < <(./run.py -T ${TASKSET} -f tmp_conf.xml -d ${BENCH_DURATION} -P ${CTRL_PORT} -x "${interested_txn}")

        if [ ${succ} -eq 1 ] && \
           [ ${#l_txns} -ne 0 ] && \
           [ ${#l_tps} -ne 0 ] && \
           [ ${#l_commits} -ne 0 ] && \
           [ ${#l_tries} -ne 0 ] && \
           [ ${#l_latency_50} -ne 0 ] && \
           [ ${#l_latency_90} -ne 0 ] && \
           [ ${#l_latency_99} -ne 0 ] && \
           [ ${#l_latency_999} -ne 0 ] && \
           [ ${#l_c_time} -ne 0 ] && \
           [ ${#l_min_latency} -ne 0 ] && \
           [ ${#l_max_latency} -ne 0 ]
        then
            # success
            sed -i "s/__kv_none\.latency_50__/${l_latency_50}/g" ${figure_gen_file}
            sed -i "s/__kv_none\.latency_90__/${l_latency_90}/g" ${figure_gen_file}
            sed -i "s/__kv_none\.latency_99__/${l_latency_99}/g" ${figure_gen_file}
            sed -i "s/__kv_none\.latency_999__/${l_latency_999}/g" ${figure_gen_file}

            tried=$((${tried}+1))
            break
        else
            # fail
            succ=0
            echo "BENCH FAIL" >&2
            echo "TRIED: $((1+${tried}))" >&2
        fi

        tried=$((${tried}+1))
    done

    local this_finish_time=`date +"%H:%M:%S"`
    local f_time_in_s=`date +"%s"`
    this_elapsed=$((${f_time_in_s}-${s_time_in_s}))
    if [ ${succ} -ne 1 ]
    then
        # failed
        echo "============== FAILURE ===============" >>${BENCHMARK_LOGGING_FILE}
        echo "BENCH FAILED FOR ${MAX_RETRY} TIMES" >>${BENCHMARK_LOGGING_FILE}
        echo "BENCHMARK     :   micro_bench (none kv latency)" >>${BENCHMARK_LOGGING_FILE}
        echo "INTERESTED TXN:   ${interested_txn}" >>${BENCHMARK_LOGGING_FILE}
        echo "MODE          :   none" >>${BENCHMARK_LOGGING_FILE}
        echo "NUM SERVS     :   1" >>${BENCHMARK_LOGGING_FILE}
        echo "NUM COOS      :   ${num_coos}" >>${BENCHMARK_LOGGING_FILE}
        echo "ELAPSED TIME  :   ${this_elapsed}" >>${BENCHMARK_LOGGING_FILE}
        echo "START TIME    :   ${this_start_time}" >>${BENCHMARK_LOGGING_FILE}
        echo "FINISH TIME   :   ${this_finish_time}" >>${BENCHMARK_LOGGING_FILE}
        echo "============== FAILURE ===============" >>${BENCHMARK_LOGGING_FILE}
        echo "" >>${BENCHMARK_LOGGING_FILE}

        sed -i "s/__kv_none\.latency_50__/-1, __kv_none\.latency_50__/g" ${figure_gen_file}
        sed -i "s/__kv_none\.latency_90__/-1, __kv_none\.latency_90__/g" ${figure_gen_file}
        sed -i "s/__kv_none\.latency_99__/-1, __kv_none\.latency_99__/g" ${figure_gen_file}
        sed -i "s/__kv_none\.latency_999__/-1, __kv_none\.latency_999__/g" ${figure_gen_file}

        # backup xml file
        mkdir -p failed_xml/micro_kv_none
        cp -f tmp_conf.xml failed_xml/micro_kv_none/${interested_txn}_lt.xml
    else
        echo "============== SUCCESS ===============" >>${BENCHMARK_LOGGING_FILE}
        echo "TRIED         :   ${tried}" >>${BENCHMARK_LOGGING_FILE}
        echo "BENCHMARK     :   micro_bench (none kv latency)" >>${BENCHMARK_LOGGING_FILE}
        echo "INTERESTED TXN:   ${interested_txn}" >>${BENCHMARK_LOGGING_FILE}
        echo "MODE          :   none" >>${BENCHMARK_LOGGING_FILE}
        echo "NUM SERVS     :   1" >>${BENCHMARK_LOGGING_FILE}
        echo "NUM COOS      :   ${num_coos}" >>${BENCHMARK_LOGGING_FILE}
        echo "ELAPSED TIME  :   ${this_elapsed}" >>${BENCHMARK_LOGGING_FILE}
        echo "START TIME    :   ${this_start_time}" >>${BENCHMARK_LOGGING_FILE}
        echo "FINISH TIME   :   ${this_finish_time}" >>${BENCHMARK_LOGGING_FILE}
        echo "============== SUCCESS ===============" >>${BENCHMARK_LOGGING_FILE}
        echo "" >>${BENCHMARK_LOGGING_FILE}
    fi

    # write for throughput
    for num_coos in $(seq ${MICRO_BENCH_MIN_CLIENTS} ${MICRO_BENCH_MAX_CLIENTS})
    do
        # TRY TO FINISH A BENCH, TILL MAX_RETRY
        local this_start_time=`date +"%H:%M:%S"`
        local s_time_in_s=`date +"%s"`

        local succ=0
        local tried=0
        while [ ${tried} -lt ${MAX_RETRY} ]
        do
            succ=0

            # PREPARE TO DO BENCHMARK
            make_bench_config ${template_file} ${mode} 1 ${num_coos} 1 >tmp_conf.xml

            # KILL UNFINISHED PROCESSES
            for serv_m in ${SERV_MACHINES[@]}
            do
                ssh ${serv_m} 'killall -9 deptran_server 2>/dev/null'
            done
            for coor_m in ${CLIENT_MACHINES[@]}
            do
                ssh ${coor_m} 'killall -9 deptran_client 2>/dev/null'
            done

            # WAITING FOR CONFIG FLUSH TO TMP_CONF.XML
            sleep 1

            local l_txns=
            local l_tries=
            local l_commits=
            local l_tps=
            local l_latency_50=
            local l_latency_90=
            local l_latency_99=
            local l_latency_999=
            local l_att_lt_50=
            local l_att_lt_90=
            local l_att_lt_99=
            local l_att_lt_999=
            local l_c_time=
            local l_min_latency=
            local l_max_latency=

            # START BENCH AND GREP RESULT
            while read line
            do
                echo "${line}"

                if [[ ${line} == "BENCHMARK_SUCCEED" ]]
                then
                    succ=1
                fi

                if [[ ${line} == *"RECORDING"* ]] && [[ ${line} == *"<${interested_txn}>"* ]]
                then
                    # grep client statistics
                    space_out=`echo ${line} | sed "s/^.*FINISHED_TXNS: \([0-9]\+\); ATTEMPTS: \([0-9]\+\); COMMITS: \([0-9]\+\); TPS: \([0-9]\+\); 50\.0% LATENCY: \([0-9]*\.\?[0-9]*\); 90\.0% LATENCY: \([0-9]*\.\?[0-9]*\); 99\.0% LATENCY: \([0-9]*\.\?[0-9]*\); 99\.9% LATENCY: \([0-9]*\.\?[0-9]*\);.*/\1 \2 \3 \4 \5 \6 \7 \8/g"`
                    read l_txns l_tries l_commits l_tps l_latency_50 l_latency_90 l_latency_99 l_latency_999 <<< "${space_out}"
                    space_out=`echo ${line} | sed "s/^.*50\.0% ATT_LT: \([0-9]*\.\?[0-9]*\); 90\.0% ATT_LT: \([0-9]*\.\?[0-9]*\); 99\.0% ATT_LT: \([0-9]*\.\?[0-9]*\); 99\.9% ATT_LT: \([0-9]*\.\?[0-9]*\); TIME: \([0-9]*\.\?[0-9]*\); LATENCY MIN: \([0-9]*\.\?[0-9]*\); LATENCY MAX: \([0-9]*\.\?[0-9]*\);.*/\1 \2 \3 \4 \5 \6 \7/g"`
                    read l_att_lt_50 l_att_lt_90 l_att_lt_99 l_att_lt_999 l_c_time l_min_latency l_max_latency <<< "${space_out}"
                fi

            done < <(./run.py -T ${TASKSET} -f tmp_conf.xml -d ${BENCH_DURATION} -P ${CTRL_PORT} -x "${interested_txn}")

            if [ ${succ} -eq 1 ] && \
               [ ${#l_txns} -ne 0 ] && \
               [ ${#l_tps} -ne 0 ] && \
               [ ${#l_commits} -ne 0 ] && \
               [ ${#l_tries} -ne 0 ] && \
               [ ${#l_latency_50} -ne 0 ] && \
               [ ${#l_latency_90} -ne 0 ] && \
               [ ${#l_latency_99} -ne 0 ] && \
               [ ${#l_latency_999} -ne 0 ] && \
               [ ${#l_c_time} -ne 0 ] && \
               [ ${#l_min_latency} -ne 0 ] && \
               [ ${#l_max_latency} -ne 0 ]
            then
                # success
                sed -i "s/__kv_none\.tps__/${l_tps}, __kv_none\.tps__/g" ${figure_gen_file}
                if [ ${l_tries} -ne 0 ]
                then
                    sed -i "s/__kv_none\.commit_rate__/${l_commits}\.0\/${l_tries}, __kv_none\.commit_rate__/g" ${figure_gen_file}
                else
                    sed -i "s/__kv_none\.commit_rate__/0\.0, __kv_none\.commit_rate__/g" ${figure_gen_file}
                fi
                sed -i "s/__kv_none\.att_latency_50__/${l_att_lt_50}, __kv_none\.att_latency_50__/g" ${figure_gen_file}
                sed -i "s/__kv_none\.att_latency_90__/${l_att_lt_90}, __kv_none\.att_latency_90__/g" ${figure_gen_file}
                sed -i "s/__kv_none\.att_latency_99__/${l_att_lt_99}, __kv_none\.att_latency_99__/g" ${figure_gen_file}
                sed -i "s/__kv_none\.att_latency_999__/${l_att_lt_999}, __kv_none\.att_latency_999__/g" ${figure_gen_file}
                sed -i "s/__kv_none\.attempts__/${l_tries}\.0\/${l_c_time}, __kv_none\.attempts__/g" ${figure_gen_file}
                sed -i "s/__kv_none\.min_latency__/${l_min_latency}, __kv_none\.min_latency__/g" ${figure_gen_file}
                sed -i "s/__kv_none\.max_latency__/${l_max_latency}, __kv_none\.max_latency__/g" ${figure_gen_file}

                tried=$((${tried}+1))
                break
            else
                # fail
                succ=0
                echo "BENCH FAIL" >&2
                echo "TRIED: $((1+${tried}))" >&2
            fi

            tried=$((${tried}+1))
        done

        local this_finish_time=`date +"%H:%M:%S"`
        local f_time_in_s=`date +"%s"`
        this_elapsed=$((${f_time_in_s}-${s_time_in_s}))
        if [ ${succ} -ne 1 ]
        then
            # failed
            echo "============== FAILURE ===============" >>${BENCHMARK_LOGGING_FILE}
            echo "BENCH FAILED FOR ${MAX_RETRY} TIMES" >>${BENCHMARK_LOGGING_FILE}
            echo "BENCHMARK     :   micro_bench (none kv)" >>${BENCHMARK_LOGGING_FILE}
            echo "INTERESTED TXN:   ${interested_txn}" >>${BENCHMARK_LOGGING_FILE}
            echo "MODE          :   none" >>${BENCHMARK_LOGGING_FILE}
            echo "NUM SERVS     :   1" >>${BENCHMARK_LOGGING_FILE}
            echo "NUM COOS      :   ${num_coos}" >>${BENCHMARK_LOGGING_FILE}
            echo "ELAPSED TIME  :   ${this_elapsed}" >>${BENCHMARK_LOGGING_FILE}
            echo "START TIME    :   ${this_start_time}" >>${BENCHMARK_LOGGING_FILE}
            echo "FINISH TIME   :   ${this_finish_time}" >>${BENCHMARK_LOGGING_FILE}
            echo "============== FAILURE ===============" >>${BENCHMARK_LOGGING_FILE}
            echo "" >>${BENCHMARK_LOGGING_FILE}

            sed -i "s/__kv_none\.tps__/-1, __kv_none\.tps__/g" ${figure_gen_file}
            sed -i "s/__kv_none\.commit_rate__/-1, __kv_none\.commit_rate__/g" ${figure_gen_file}
            sed -i "s/__kv_none\.att_latency_50__/-1, __kv_none\.att_latency_50__/g" ${figure_gen_file}
            sed -i "s/__kv_none\.att_latency_90__/-1, __kv_none\.att_latency_90__/g" ${figure_gen_file}
            sed -i "s/__kv_none\.att_latency_99__/-1, __kv_none\.att_latency_99__/g" ${figure_gen_file}
            sed -i "s/__kv_none\.att_latency_999__/-1, __kv_none\.att_latency_999__/g" ${figure_gen_file}
            sed -i "s/__kv_none\.attempts__/-1, __kv_none\.attempts__/g" ${figure_gen_file}

            sed -i "s/__kv_none\.min_latency__/-1, __kv_none\.min_latency__/g" ${figure_gen_file}
            sed -i "s/__kv_none\.max_latency__/-1, __kv_none\.max_latency__/g" ${figure_gen_file}

            # backup xml file
            mkdir -p failed_xml/micro_kv_none
            cp -f tmp_conf.xml failed_xml/micro_kv_none/${num_coos}coos.xml
        else
            echo "============== SUCCESS ===============" >>${BENCHMARK_LOGGING_FILE}
            echo "TRIED         :   ${tried}" >>${BENCHMARK_LOGGING_FILE}
            echo "BENCHMARK     :   micro_bench (none kv)" >>${BENCHMARK_LOGGING_FILE}
            echo "INTERESTED TXN:   ${interested_txn}" >>${BENCHMARK_LOGGING_FILE}
            echo "MODE          :   none" >>${BENCHMARK_LOGGING_FILE}
            echo "NUM SERVS     :   1" >>${BENCHMARK_LOGGING_FILE}
            echo "NUM COOS      :   ${num_coos}" >>${BENCHMARK_LOGGING_FILE}
            echo "ELAPSED TIME  :   ${this_elapsed}" >>${BENCHMARK_LOGGING_FILE}
            echo "START TIME    :   ${this_start_time}" >>${BENCHMARK_LOGGING_FILE}
            echo "FINISH TIME   :   ${this_finish_time}" >>${BENCHMARK_LOGGING_FILE}
            echo "============== SUCCESS ===============" >>${BENCHMARK_LOGGING_FILE}
            echo "" >>${BENCHMARK_LOGGING_FILE}
        fi

        sed -i "s/__kv_none\.num_coos__/${num_coos}, __kv_none\.num_coos__/g" ${figure_gen_file}

    done


    sed -i "s/, __kv_none\.tps__//g" ${figure_gen_file}
    sed -i "s/, __kv_none\.commit_rate__//g" ${figure_gen_file}
    sed -i "s/, __kv_none\.att_latency_50__//g" ${figure_gen_file}
    sed -i "s/, __kv_none\.att_latency_90__//g" ${figure_gen_file}
    sed -i "s/, __kv_none\.att_latency_99__//g" ${figure_gen_file}
    sed -i "s/, __kv_none\.att_latency_999__//g" ${figure_gen_file}
    sed -i "s/, __kv_none\.attempts__//g" ${figure_gen_file}
    sed -i "s/, __kv_none\.min_latency__//g" ${figure_gen_file}
    sed -i "s/, __kv_none\.max_latency__//g" ${figure_gen_file}
    sed -i "s/, __kv_none\.num_coos__//g" ${figure_gen_file}

    rm -f tmp_conf.xml 2>/dev/null
}

do_micro_txn_bench()
{
    local figure_gen_file=$1
    local template_file="./template/micro_bench.xml"

    local num_coos=1
    local interested_txn="PAYMENT"
    for mode in ${MICRO_MODES[@]}
    do
        # TRY TO FINISH A BENCH, TILL MAX_RETRY
        local this_start_time=`date +"%H:%M:%S"`
        local s_time_in_s=`date +"%s"`

        local succ=0
        local tried=0
        while [ ${tried} -lt ${MAX_RETRY} ]
        do
            succ=0

            # PREPARE TO DO BENCHMARK
            make_bench_config ${template_file} ${mode} ${MICRO_BENCH_NUM_SERVS} ${num_coos} ${MICRO_BENCH_NUM_SERVS} >tmp_conf.xml

            # KILL UNFINISHED PROCESSES
            for serv_m in ${SERV_MACHINES[@]}
            do
                ssh ${serv_m} 'killall -9 deptran_server 2>/dev/null'
            done
            for coor_m in ${CLIENT_MACHINES[@]}
            do
                ssh ${coor_m} 'killall -9 deptran_client 2>/dev/null'
            done

            # WAITING FOR CONFIG FLUSH TO TMP_CONF.XML
            sleep 1

            local l_txns=
            local l_tries=
            local l_commits=
            local l_tps=
            local l_latency_50=
            local l_latency_90=
            local l_latency_99=
            local l_latency_999=
            local l_att_lt_50=
            local l_att_lt_90=
            local l_att_lt_99=
            local l_att_lt_999=
            local l_c_time=
            local l_min_latency=
            local l_max_latency=

            local l_graph=
            local l_ask=
            local l_scc=

            # START BENCH AND GREP RESULT
            while read line
            do
                echo "${line}"

                if [[ ${line} == "BENCHMARK_SUCCEED" ]]
                then
                    succ=1
                fi

                if [[ ${line} == *"RECORDING"* ]] && [[ ${line} == *"<${interested_txn}>"* ]]
                then
                    # grep client statistics
                    space_out=`echo ${line} | sed "s/^.*FINISHED_TXNS: \([0-9]\+\); ATTEMPTS: \([0-9]\+\); COMMITS: \([0-9]\+\); TPS: \([0-9]\+\); 50\.0% LATENCY: \([0-9]*\.\?[0-9]*\); 90\.0% LATENCY: \([0-9]*\.\?[0-9]*\); 99\.0% LATENCY: \([0-9]*\.\?[0-9]*\); 99\.9% LATENCY: \([0-9]*\.\?[0-9]*\);.*/\1 \2 \3 \4 \5 \6 \7 \8/g"`
                    read l_txns l_tries l_commits l_tps l_latency_50 l_latency_90 l_latency_99 l_latency_999 <<< "${space_out}"
                    space_out=`echo ${line} | sed "s/^.*50\.0% ATT_LT: \([0-9]*\.\?[0-9]*\); 90\.0% ATT_LT: \([0-9]*\.\?[0-9]*\); 99\.0% ATT_LT: \([0-9]*\.\?[0-9]*\); 99\.9% ATT_LT: \([0-9]*\.\?[0-9]*\); TIME: \([0-9]*\.\?[0-9]*\); LATENCY MIN: \([0-9]*\.\?[0-9]*\); LATENCY MAX: \([0-9]*\.\?[0-9]*\);.*/\1 \2 \3 \4 \5 \6 \7/g"`
                    read l_att_lt_50 l_att_lt_90 l_att_lt_99 l_att_lt_999 l_c_time l_min_latency l_max_latency <<< "${space_out}"
                elif [[ ${line} == *"SERVREC"* ]]
                then
                    # grep server statistics
                    space_out=`echo ${line} | sed "s/^.*SERVREC: \([a-zA-Z]\+\): VALUE: \([0-9]\+\); TIMES: \([0-9]\+\); MEAN: \([0-9]*\.\?[0-9]*\); TIME: \([0-9]*\.\?[0-9]*\)/\1 \2 \3 \4 \5/g"`
                    read key value timez mean s_time <<< "${space_out}"
                    if [ ${key} == "graph" ]
                    then
                        l_graph=${mean}
                    elif [ ${key} == "ask" ]
                    then
                        l_ask="${value}\\/${s_time}"
                    elif [ ${key} == "scc" ]
                    then
                        l_scc=${mean}
                    fi
                fi

            done < <(./run.py -T ${TASKSET} -f tmp_conf.xml -d ${BENCH_DURATION} -P ${CTRL_PORT} -x "${interested_txn}")

            if [ ${succ} -eq 1 ] && \
               [ ${#l_txns} -ne 0 ] && \
               [ ${#l_tps} -ne 0 ] && \
               [ ${#l_commits} -ne 0 ] && \
               [ ${#l_tries} -ne 0 ] && \
               [ ${#l_latency_50} -ne 0 ] && \
               [ ${#l_latency_90} -ne 0 ] && \
               [ ${#l_latency_99} -ne 0 ] && \
               [ ${#l_latency_999} -ne 0 ] && \
               [ ${#l_c_time} -ne 0 ] && \
               [ ${#l_min_latency} -ne 0 ] && \
               [ ${#l_max_latency} -ne 0 ]
            then
                # success
                sed -i "s/__${mode}\.latency_50__/${l_latency_50}/g" ${figure_gen_file}
                sed -i "s/__${mode}\.latency_90__/${l_latency_90}/g" ${figure_gen_file}
                sed -i "s/__${mode}\.latency_99__/${l_latency_99}/g" ${figure_gen_file}
                sed -i "s/__${mode}\.latency_999__/${l_latency_999}/g" ${figure_gen_file}

                tried=$((${tried}+1))
                break
            else
                # fail
                succ=0
                echo "BENCH FAIL" >&2
                echo "TRIED: $((1+${tried}))" >&2
            fi

            tried=$((${tried}+1))
        done

        local this_finish_time=`date +"%H:%M:%S"`
        local f_time_in_s=`date +"%s"`
        this_elapsed=$((${f_time_in_s}-${s_time_in_s}))
        if [ ${succ} -ne 1 ]
        then
            # failed
            echo "============== FAILURE ===============" >>${BENCHMARK_LOGGING_FILE}
            echo "BENCH FAILED FOR ${MAX_RETRY} TIMES" >>${BENCHMARK_LOGGING_FILE}
            echo "BENCHMARK     :   micro_bench (4-piece txn latency)" >>${BENCHMARK_LOGGING_FILE}
            echo "INTERESTED TXN:   ${interested_txn}" >>${BENCHMARK_LOGGING_FILE}
            echo "MODE          :   ${mode}" >>${BENCHMARK_LOGGING_FILE}
            echo "NUM SERVS     :   ${MICRO_BENCH_NUM_SERVS}" >>${BENCHMARK_LOGGING_FILE}
            echo "NUM COOS      :   ${num_coos}" >>${BENCHMARK_LOGGING_FILE}
            echo "ELAPSED TIME  :   ${this_elapsed}" >>${BENCHMARK_LOGGING_FILE}
            echo "START TIME    :   ${this_start_time}" >>${BENCHMARK_LOGGING_FILE}
            echo "FINISH TIME   :   ${this_finish_time}" >>${BENCHMARK_LOGGING_FILE}
            echo "============== FAILURE ===============" >>${BENCHMARK_LOGGING_FILE}
            echo "" >>${BENCHMARK_LOGGING_FILE}

            sed -i "s/__${mode}\.latency_50__/-1, __${mode}\.latency_50__/g" ${figure_gen_file}
            sed -i "s/__${mode}\.latency_90__/-1, __${mode}\.latency_90__/g" ${figure_gen_file}
            sed -i "s/__${mode}\.latency_99__/-1, __${mode}\.latency_99__/g" ${figure_gen_file}
            sed -i "s/__${mode}\.latency_999__/-1, __${mode}\.latency_999__/g" ${figure_gen_file}

            # backup xml file
            mkdir -p failed_xml/micro_bench
            cp -f tmp_conf.xml failed_xml/micro_bench/${mode}_${interested_txn}_lt.xml
        else
            echo "============== SUCCESS ===============" >>${BENCHMARK_LOGGING_FILE}
            echo "TRIED         :   ${tried}" >>${BENCHMARK_LOGGING_FILE}
            echo "BENCHMARK     :   micro_bench (4-piece txn latency)" >>${BENCHMARK_LOGGING_FILE}
            echo "INTERESTED TXN:   ${interested_txn}" >>${BENCHMARK_LOGGING_FILE}
            echo "MODE          :   ${mode}" >>${BENCHMARK_LOGGING_FILE}
            echo "NUM SERVS     :   ${MICRO_BENCH_NUM_SERVS}" >>${BENCHMARK_LOGGING_FILE}
            echo "NUM COOS      :   ${num_coos}" >>${BENCHMARK_LOGGING_FILE}
            echo "ELAPSED TIME  :   ${this_elapsed}" >>${BENCHMARK_LOGGING_FILE}
            echo "START TIME    :   ${this_start_time}" >>${BENCHMARK_LOGGING_FILE}
            echo "FINISH TIME   :   ${this_finish_time}" >>${BENCHMARK_LOGGING_FILE}
            echo "============== SUCCESS ===============" >>${BENCHMARK_LOGGING_FILE}
            echo "" >>${BENCHMARK_LOGGING_FILE}
        fi
    done

    # write for throughput
    for num_coos in $(seq ${MICRO_BENCH_MIN_COOS} ${MICRO_BENCH_MAX_COOS})
    do
        for mode in ${MICRO_MODES[@]}
        do
            # TRY TO FINISH A BENCH, TILL MAX_RETRY
            local this_start_time=`date +"%H:%M:%S"`
            local s_time_in_s=`date +"%s"`

            local succ=0
            local tried=0
            while [ ${tried} -lt ${MAX_RETRY} ]
            do
                succ=0

                # PREPARE TO DO BENCHMARK
                make_bench_config ${template_file} ${mode} ${MICRO_BENCH_NUM_SERVS} ${num_coos} ${MICRO_BENCH_NUM_SERVS} >tmp_conf.xml

                # KILL UNFINISHED PROCESSES
                for serv_m in ${SERV_MACHINES[@]}
                do
                    ssh ${serv_m} 'killall -9 deptran_server 2>/dev/null'
                done
                for coor_m in ${CLIENT_MACHINES[@]}
                do
                    ssh ${coor_m} 'killall -9 deptran_client 2>/dev/null'
                done

                # WAITING FOR CONFIG FLUSH TO TMP_CONF.XML
                sleep 1

                local l_txns=
                local l_tries=
                local l_commits=
                local l_tps=
                local l_latency_50=
                local l_latency_90=
                local l_latency_99=
                local l_latency_999=
                local l_att_lt_50=
                local l_att_lt_90=
                local l_att_lt_99=
                local l_att_lt_999=
                local l_c_time=
                local l_min_latency=
                local l_max_latency=

                local l_graph=
                local l_ask=
                local l_scc=

                # START BENCH AND GREP RESULT
                while read line
                do
                    echo "${line}"

                    if [[ ${line} == "BENCHMARK_SUCCEED" ]]
                    then
                        succ=1
                    fi

                    if [[ ${line} == *"RECORDING"* ]] && [[ ${line} == *"<${interested_txn}>"* ]]
                    then
                        # grep client statistics
                        space_out=`echo ${line} | sed "s/^.*FINISHED_TXNS: \([0-9]\+\); ATTEMPTS: \([0-9]\+\); COMMITS: \([0-9]\+\); TPS: \([0-9]\+\); 50\.0% LATENCY: \([0-9]*\.\?[0-9]*\); 90\.0% LATENCY: \([0-9]*\.\?[0-9]*\); 99\.0% LATENCY: \([0-9]*\.\?[0-9]*\); 99\.9% LATENCY: \([0-9]*\.\?[0-9]*\);.*/\1 \2 \3 \4 \5 \6 \7 \8/g"`
                        read l_txns l_tries l_commits l_tps l_latency_50 l_latency_90 l_latency_99 l_latency_999 <<< "${space_out}"
                        space_out=`echo ${line} | sed "s/^.*50\.0% ATT_LT: \([0-9]*\.\?[0-9]*\); 90\.0% ATT_LT: \([0-9]*\.\?[0-9]*\); 99\.0% ATT_LT: \([0-9]*\.\?[0-9]*\); 99\.9% ATT_LT: \([0-9]*\.\?[0-9]*\); TIME: \([0-9]*\.\?[0-9]*\); LATENCY MIN: \([0-9]*\.\?[0-9]*\); LATENCY MAX: \([0-9]*\.\?[0-9]*\);.*/\1 \2 \3 \4 \5 \6 \7/g"`
                        read l_att_lt_50 l_att_lt_90 l_att_lt_99 l_att_lt_999 l_c_time l_min_latency l_max_latency <<< "${space_out}"
                    elif [[ ${line} == *"SERVREC"* ]]
                    then
                        # grep server statistics
                        space_out=`echo ${line} | sed "s/^.*SERVREC: \([a-zA-Z]\+\): VALUE: \([0-9]\+\); TIMES: \([0-9]\+\); MEAN: \([0-9]*\.\?[0-9]*\); TIME: \([0-9]*\.\?[0-9]*\)/\1 \2 \3 \4 \5/g"`
                        read key value timez mean s_time <<< "${space_out}"
                        if [ ${key} == "graph" ]
                        then
                            l_graph=${mean}
                        elif [ ${key} == "ask" ]
                        then
                            l_ask="${value}\\/${s_time}"
                        elif [ ${key} == "scc" ]
                        then
                            l_scc=${mean}
                        fi
                    fi

                done < <(./run.py -T ${TASKSET} -f tmp_conf.xml -d ${BENCH_DURATION} -P ${CTRL_PORT} -x "${interested_txn}")

                if [ ${succ} -eq 1 ] && \
                   [ ${#l_txns} -ne 0 ] && \
                   [ ${#l_tps} -ne 0 ] && \
                   [ ${#l_commits} -ne 0 ] && \
                   [ ${#l_tries} -ne 0 ] && \
                   [ ${#l_latency_50} -ne 0 ] && \
                   [ ${#l_latency_90} -ne 0 ] && \
                   [ ${#l_latency_99} -ne 0 ] && \
                   [ ${#l_latency_999} -ne 0 ] && \
                   [ ${#l_c_time} -ne 0 ] && \
                   [ ${#l_min_latency} -ne 0 ] && \
                   [ ${#l_max_latency} -ne 0 ]
                then
                    # success
                    sed -i "s/__${mode}\.tps__/${l_tps}, __${mode}\.tps__/g" ${figure_gen_file}
                    if [ ${l_tries} -ne 0 ]
                    then
                        sed -i "s/__${mode}\.commit_rate__/${l_commits}\.0\/${l_tries}, __${mode}\.commit_rate__/g" ${figure_gen_file}
                    else
                        sed -i "s/__${mode}\.commit_rate__/0\.0, __${mode}\.commit_rate__/g" ${figure_gen_file}
                    fi
                    sed -i "s/__${mode}\.att_latency_50__/${l_att_lt_50}, __${mode}\.att_latency_50__/g" ${figure_gen_file}
                    sed -i "s/__${mode}\.att_latency_90__/${l_att_lt_90}, __${mode}\.att_latency_90__/g" ${figure_gen_file}
                    sed -i "s/__${mode}\.att_latency_99__/${l_att_lt_99}, __${mode}\.att_latency_99__/g" ${figure_gen_file}
                    sed -i "s/__${mode}\.att_latency_999__/${l_att_lt_999}, __${mode}\.att_latency_999__/g" ${figure_gen_file}
                    sed -i "s/__${mode}\.attempts__/${l_tries}\.0\/${l_c_time}, __${mode}\.attempts__/g" ${figure_gen_file}

                    if [ ${#l_graph} -ne 0 ]
                    then
                        sed -i "s/__${mode}\.graph_size__/${l_graph}, __${mode}\.graph_size__/g" ${figure_gen_file}
                    else
                        sed -i "s/__${mode}\.graph_size__/0\.0, __${mode}\.graph_size__/g" ${figure_gen_file}
                    fi
                    if [ ${#l_ask} -ne 0 ]
                    then
                        sed -i "s/__${mode}\.n_ask__/${l_ask}, __${mode}\.n_ask__/g" ${figure_gen_file}
                    else
                        sed -i "s/__${mode}\.n_ask__/0\.0, __${mode}\.n_ask__/g" ${figure_gen_file}
                    fi
                    if [ ${#l_scc} -ne 0 ]
                    then
                        sed -i "s/__${mode}\.scc__/${l_scc}, __${mode}\.scc__/g" ${figure_gen_file}
                    else
                        sed -i "s/__${mode}\.scc__/0\.0, __${mode}\.scc__/g" ${figure_gen_file}
                    fi
                    sed -i "s/__${mode}\.min_latency__/${l_min_latency}, __${mode}\.min_latency__/g" ${figure_gen_file}
                    sed -i "s/__${mode}\.max_latency__/${l_max_latency}, __${mode}\.max_latency__/g" ${figure_gen_file}

                    tried=$((${tried}+1))
                    break
                else
                    # fail
                    succ=0
                    echo "BENCH FAIL" >&2
                    echo "TRIED: $((1+${tried}))" >&2
                fi

                tried=$((${tried}+1))
            done

            local this_finish_time=`date +"%H:%M:%S"`
            local f_time_in_s=`date +"%s"`
            this_elapsed=$((${f_time_in_s}-${s_time_in_s}))
            if [ ${succ} -ne 1 ]
            then
                # failed
                echo "============== FAILURE ===============" >>${BENCHMARK_LOGGING_FILE}
                echo "BENCH FAILED FOR ${MAX_RETRY} TIMES" >>${BENCHMARK_LOGGING_FILE}
                echo "BENCHMARK     :   micro_bench (4-piece txn)" >>${BENCHMARK_LOGGING_FILE}
                echo "INTERESTED TXN:   ${interested_txn}" >>${BENCHMARK_LOGGING_FILE}
                echo "MODE          :   ${mode}" >>${BENCHMARK_LOGGING_FILE}
                echo "NUM SERVS     :   ${MICRO_BENCH_NUM_SERVS}" >>${BENCHMARK_LOGGING_FILE}
                echo "NUM COOS      :   ${num_coos}" >>${BENCHMARK_LOGGING_FILE}
                echo "ELAPSED TIME  :   ${this_elapsed}" >>${BENCHMARK_LOGGING_FILE}
                echo "START TIME    :   ${this_start_time}" >>${BENCHMARK_LOGGING_FILE}
                echo "FINISH TIME   :   ${this_finish_time}" >>${BENCHMARK_LOGGING_FILE}
                echo "============== FAILURE ===============" >>${BENCHMARK_LOGGING_FILE}
                echo "" >>${BENCHMARK_LOGGING_FILE}

                sed -i "s/__${mode}\.tps__/-1, __${mode}\.tps__/g" ${figure_gen_file}
                sed -i "s/__${mode}\.commit_rate__/-1, __${mode}\.commit_rate__/g" ${figure_gen_file}
                sed -i "s/__${mode}\.att_latency_50__/-1, __${mode}\.att_latency_50__/g" ${figure_gen_file}
                sed -i "s/__${mode}\.att_latency_90__/-1, __${mode}\.att_latency_90__/g" ${figure_gen_file}
                sed -i "s/__${mode}\.att_latency_99__/-1, __${mode}\.att_latency_99__/g" ${figure_gen_file}
                sed -i "s/__${mode}\.att_latency_999__/-1, __${mode}\.att_latency_999__/g" ${figure_gen_file}
                sed -i "s/__${mode}\.attempts__/-1, __${mode}\.attempts__/g" ${figure_gen_file}

                sed -i "s/__${mode}\.graph_size__/-1, __${mode}\.graph_size__/g" ${figure_gen_file}
                sed -i "s/__${mode}\.n_ask__/-1, __${mode}\.n_ask__/g" ${figure_gen_file}
                sed -i "s/__${mode}\.scc__/-1, __${mode}\.scc__/g" ${figure_gen_file}
                sed -i "s/__${mode}\.min_latency__/-1, __${mode}\.min_latency__/g" ${figure_gen_file}
                sed -i "s/__${mode}\.max_latency__/-1, __${mode}\.max_latency__/g" ${figure_gen_file}

                # backup xml file
                mkdir -p failed_xml/micro_bench
                cp -f tmp_conf.xml failed_xml/micro_bench/${mode}_${num_coos}coos.xml
            else
                echo "============== SUCCESS ===============" >>${BENCHMARK_LOGGING_FILE}
                echo "TRIED         :   ${tried}" >>${BENCHMARK_LOGGING_FILE}
                echo "BENCHMARK     :   micro_bench (4-piece txn)" >>${BENCHMARK_LOGGING_FILE}
                echo "INTERESTED TXN:   ${interested_txn}" >>${BENCHMARK_LOGGING_FILE}
                echo "MODE          :   ${mode}" >>${BENCHMARK_LOGGING_FILE}
                echo "NUM SERVS     :   ${MICRO_BENCH_NUM_SERVS}" >>${BENCHMARK_LOGGING_FILE}
                echo "NUM COOS      :   ${num_coos}" >>${BENCHMARK_LOGGING_FILE}
                echo "ELAPSED TIME  :   ${this_elapsed}" >>${BENCHMARK_LOGGING_FILE}
                echo "START TIME    :   ${this_start_time}" >>${BENCHMARK_LOGGING_FILE}
                echo "FINISH TIME   :   ${this_finish_time}" >>${BENCHMARK_LOGGING_FILE}
                echo "============== SUCCESS ===============" >>${BENCHMARK_LOGGING_FILE}
                echo "" >>${BENCHMARK_LOGGING_FILE}
            fi
        done

        # add x axis
        sed -i "s/__micro_bench\.num_coos__/${num_coos}, __micro_bench\.num_coos__/g" ${figure_gen_file}

        num_coos=$((${num_coos}+1))
    done

    sed -i "s/, __micro_bench\.num_coos__//g" ${figure_gen_file}

    for mode in ${MICRO_MODES[@]}
    do
        sed -i "s/, __${mode}\.tps__//g" ${figure_gen_file}
        sed -i "s/, __${mode}\.commit_rate__//g" ${figure_gen_file}
        sed -i "s/, __${mode}\.att_latency_50__//g" ${figure_gen_file}
        sed -i "s/, __${mode}\.att_latency_90__//g" ${figure_gen_file}
        sed -i "s/, __${mode}\.att_latency_99__//g" ${figure_gen_file}
        sed -i "s/, __${mode}\.att_latency_999__//g" ${figure_gen_file}
        sed -i "s/, __${mode}\.attempts__//g" ${figure_gen_file}
        sed -i "s/, __${mode}\.graph_size__//g" ${figure_gen_file}
        sed -i "s/, __${mode}\.n_ask__//g" ${figure_gen_file}
        sed -i "s/, __${mode}\.scc__//g" ${figure_gen_file}
        sed -i "s/, __${mode}\.min_latency__//g" ${figure_gen_file}
        sed -i "s/, __${mode}\.max_latency__//g" ${figure_gen_file}
    done

    rm -f tmp_conf.xml 2>/dev/null
}

do_micro_bench()
{
    local figure_gen_file=scripts/micro_bench_figure_gen_local.py
    cp -f template/micro_bench_figure_gen_template.py ${figure_gen_file}

    do_null_rpc_bench ${figure_gen_file}

    do_kv_bench ${figure_gen_file}

    do_micro_txn_bench ${figure_gen_file}
}

gen_config()
{
    rm -rf conf
    mkdir conf
    for bench in ${BENCHS[@]}
    do
        mkdir -p conf/${bench}
        local template_file="./template/${bench}.xml"

        local coo_scale=${MIN_COO_SCALE}
        local num_coos=${COOS_START}
        while [ ${coo_scale} -le ${MAX_COO_SCALE} ]
        do
            for mode in ${MODES[@]}
            do
                local total_num_coos=$((${num_coos}*${NUM_SERVS}))
                make_bench_config ${template_file} ${mode} ${NUM_SERVS} ${total_num_coos} ${NUM_SERVS} >conf/${bench}/${mode}_${num_coos}coos.xml
            done

            if [ ${num_coos} -lt $((${COOS_COARSE_UNIT}*${MIN_COO_SCALE})) ]
            then
                num_coos=$((${num_coos}+1))
            else
                coo_scale=$((${coo_scale}+1))
                num_coos=$((${COOS_COARSE_UNIT}*${coo_scale}))
            fi
        done
    done
}

start_bench() {
    mkdir scripts
    local common_bench=${1}
    local scale=${2}
    local micro=${3}
    rm -f ${BENCHMARK_LOGGING_FILE}
    touch ${BENCHMARK_LOGGING_FILE}

    if [ ${scale} -eq 1 ]
    then
        local power=1
        for dummy in $(seq 1 ${SCALE_MAX_SERV_SCALE})
        do
            power=$((${power}*2))
        done
        local n_max_s=$((${power}*${SCALE_NUM_SERVS_START}))
        if [ ${n_max_s} -gt $((${#SERV_MACHINES[@]}*${SERVS_PER_MACHINE})) ]
        then
            echo "" >&2
            echo "Server machine not enough" >&2
            echo "" >&2
            exit 2
        fi
        do_scaling_bench
    fi

    if [ ${common_bench} -eq 1 ]
    then
        if [ 0 -ne $((${NUM_SERVS}%${SERVS_PER_MACHINE})) ]
        then
            echo "" >&2
            echo "Total number of servers should be divided by servers per machine" >&2
            echo "" >&2
            exit 1
        fi

        if [ ${#SERV_MACHINES[@]} -lt $((${NUM_SERVS}/${SERVS_PER_MACHINE})) ]
        then
            echo "" >&2
            echo "Server machine not enough" >&2
            echo "" >&2
            exit 2
        fi
        do_common_bench
    fi

    if [ ${micro} -eq 1 ]
    then
        do_micro_bench
    fi
}

coos_needed=$((${NUM_SERVS}*${MAX_COO_SCALE}*${COOS_COARSE_UNIT}))
coos=$((${COOS_PER_CLIENT}*${MAX_CLIENT_PROCESSES}*${#CLIENT_MACHINES[@]}))
if [ ${coos} -lt ${coos_needed} ]
then
    echo "" >&2
    echo "Client machine not enough, need to start ${coos_needed} coordinators," >&2
    echo "Can start ${coos} for the current config" >&2
    echo "" >&2
    exit 1
fi
if [ 1 -eq ${GEN_CONFIG} ]
then
    if [ 0 -ne $((${NUM_SERVS}%${SERVS_PER_MACHINE})) ]
    then
        echo "" >&2
        echo "Total number of servers should be divided by servers per machine" >&2
        echo "" >&2
        exit 1
    fi

    if [ ${#SERV_MACHINES[@]} -lt $((${NUM_SERVS}/${SERVS_PER_MACHINE})) ]
    then
        echo "" >&2
        echo "Server machine not enough" >&2
        echo "" >&2
        exit 2
    fi
    gen_config

else
    start_bench ${DO_COMMON_BENCH} ${DO_SCALE_BENCH} ${DO_MICRO_BENCH}
fi
