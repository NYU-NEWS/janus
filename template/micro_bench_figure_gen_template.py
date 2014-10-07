#! /usr/bin/env python

import figure

figure.kv_xxx = [__kv_none.num_coos__]
figure.rpc_xxx = [__null_rpc.num_coos__]
figure.micro_bench_xxx = [__micro_bench.num_coos__]

if __name__ == "__main__":

    rpc_cpu_val = [__null_rpc.cpu__]
    rpc_throughput_val = [__null_rpc.qps__]

    rpc_latency_50_val = __null_rpc.latency_50__

    rpc_latency_90_val = __null_rpc.latency_90__

    rpc_latency_99_val = __null_rpc.latency_99__

    rpc_latency_999_val = __null_rpc.latency_999__

    cpu_val = [__kv_none.cpu__]
    kv_throughput_val = [__kv_none.tps__]

    #kv_commit_rate_val = [__kv_none.commit_rate__]

    kv_latency_50_val = __kv_none.latency_50__

    kv_latency_90_val = __kv_none.latency_90__

    kv_latency_99_val = __kv_none.latency_99__

    kv_latency_999_val = __kv_none.latency_999__

    #kv_att_latency_50_val = [__kv_none.att_latency_50__]

    #kv_att_latency_90_val = [__kv_none.att_latency_90__]

    #kv_att_latency_99_val = [__kv_none.att_latency_99__]

    #kv_att_latency_999_val = [__kv_none.att_latency_999__]

    #kv_latency_min_val = [__kv_none.min_latency__]

    #kv_latency_max_val = [__kv_none.max_latency__]

    #kv_attempts_val = [__kv_none.attempts__]

    cpu_val = [
            [__none.cpu__], # none
            [__occ.cpu__], # occ
            [__deptran.cpu__], # deptran
            [__2pl_wound_die.cpu__] # 2pl
            ]

    throughput_val = [
            [__none.tps__], # none
            [__occ.tps__], # occ
            [__deptran.tps__], # deptran
            [__2pl_wound_die.tps__] # 2pl
            ]

    #commit_rate_val = [
    #        [__none.commit_rate__],
    #        [__occ.commit_rate__],
    #        [__deptran.commit_rate__],
    #        [__2pl_wound_die.commit_rate__]
    #        ]

    latency_50_val = [
            __none.latency_50__, # none
            __occ.latency_50__, # occ
            __deptran.latency_50__, # deptran
            __2pl_wound_die.latency_50__ # 2pl
            ]

    latency_90_val = [
            __none.latency_90__, # none
            __occ.latency_90__, # occ
            __deptran.latency_90__, # deptran
            __2pl_wound_die.latency_90__ # 2pl
            ]

    latency_99_val = [
            __none.latency_99__, # none
            __occ.latency_99__, # occ
            __deptran.latency_99__, # deptran
            __2pl_wound_die.latency_99__ # 2pl
            ]

    latency_999_val = [
            __none.latency_999__, # none
            __occ.latency_999__, # occ
            __deptran.latency_999__, # deptran
            __2pl_wound_die.latency_999__ # 2pl
            ]

    #att_latency_50_val = [
    #        [__none.latency_50__],
    #        [__occ.att_latency_50__],
    #        [__deptran.att_latency_50__],
    #        [__2pl_wound_die.att_latency_50__]
    #        ]

    #att_latency_90_val = [
    #        [__none.latency_90__],
    #        [__occ.att_latency_90__],
    #        [__deptran.att_latency_90__],
    #        [__2pl_wound_die.att_latency_90__]
    #        ]

    #att_latency_99_val = [
    #        [__none.latency_99__],
    #        [__occ.att_latency_99__],
    #        [__deptran.att_latency_99__],
    #        [__2pl_wound_die.att_latency_99__]
    #        ]

    #att_latency_999_val = [
    #        [__none.latency_999__],
    #        [__occ.att_latency_999__],
    #        [__deptran.att_latency_999__],
    #        [__2pl_wound_die.att_latency_999__]
    #        ]

    #latency_min_val = [
    #        [__none.min_latency__],
    #        [__occ.min_latency__],
    #        [__deptran.min_latency__],
    #        [__2pl_wound_die.min_latency__]
    #        ]

    #latency_max_val = [
    #        [__none.max_latency__],
    #        [__occ.max_latency__],
    #        [__deptran.max_latency__],
    #        [__2pl_wound_die.max_latency__]
    #        ]

    #attempts_val = [
    #        [__none.attempts__],
    #        [__occ.attempts__],
    #        [__deptran.attempts__],
    #        [__2pl_wound_die.attempts__]
    #        ]

    #graph_size_val = [
    #        [__deptran.graph_size__]
    #        ]

    #n_ask_val = [
    #        [__deptran.n_ask__]
    #        ]

    #scc_val = [
    #        [__deptran.scc__]
    #        ]


    rpc_throughput_val.sort()
    rpc_tp = rpc_throughput_val[len(rpc_throughput_val) - 1]

    kv_throughput_val.sort()
    kv_tp = kv_throughput_val[len(kv_throughput_val) - 1]

    throughput_val[0].sort() # none
    tp_none = throughput_val[0][len(throughput_val[0]) - 1]
    throughput_val[1].sort() # occ
    tp_occ = throughput_val[1][len(throughput_val[1]) - 1]
    throughput_val[2].sort() # deptran
    tp_deptran = throughput_val[2][len(throughput_val[2]) - 1]
    throughput_val[3].sort() # 2pl
    tp_2pl = throughput_val[3][len(throughput_val[3]) - 1]

    xs = ['null rpc', 'kv', 'micro / none', 'micro / occ', 'micro / 2pl', 'micro / deptran']
    throughputs = [rpc_tp, kv_tp, tp_none, tp_occ, tp_2pl, tp_deptran]

    throughput_figname = "fig/micro_tp.pdf"

    figure.micro_tp(xs, throughputs, throughput_figname);

    print "1 RPC\t& " +   str(round(rpc_latency_50_val, 3)) + " & " + str(round(rpc_latency_90_val, 3)) + " & " + str(round(rpc_latency_99_val, 3)) + " & " + str(round(rpc_latency_999_val, 3)) + " \\\\"
    print "1 RPC+DB\t& " +         str(round(kv_latency_50_val, 3)) + " & " + str(round(kv_latency_90_val, 3)) + " & " + str(round(kv_latency_99_val, 3)) + " & " + str(round(kv_latency_999_val, 3)) + " \\\\"
    print "3 RPC+DB\t& " +       str(round(latency_50_val[0], 3)) + " & " + str(round(latency_90_val[0], 3)) + " & " + str(round(latency_99_val[0], 3)) + " & " + str(round(latency_999_val[0], 3)) + " \\\\"
    print "OCC\t& "  +       str(round(latency_50_val[1], 3)) + " & " + str(round(latency_90_val[1], 3)) + " & " + str(round(latency_99_val[1], 3)) + " & " + str(round(latency_999_val[1], 3)) + " \\\\"
    print "2PL\t& "  +       str(round(latency_50_val[3], 3)) + " & " + str(round(latency_90_val[3], 3)) + " & " + str(round(latency_99_val[3], 3)) + " & " + str(round(latency_999_val[3], 3)) + " \\\\"
    print "{\\DEPTRAN}\t& " +    str(round(latency_50_val[2], 3)) + " & " + str(round(latency_90_val[2], 3)) + " & " + str(round(latency_99_val[2], 3)) + " & " + str(round(latency_999_val[2], 3)) + " \\\\"

#    print("Hello")

