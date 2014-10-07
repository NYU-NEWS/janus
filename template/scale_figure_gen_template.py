#! /usr/bin/env python

import figure

figure.xxx = [__num_servs__]

if __name__ == "__main__":

    cpu = [
            [__occ.cpu__], # occ
            [__deptran.cpu__], # rococo
            #[__2pl.cpu__], # 2pl timeout
            #[__2pl_wait_die.cpu__], # 2pl wait die
            [__2pl_wound_die.cpu__] # 2pl wound wait
            ]

    flush_cnt = [
            [__occ.flush_cnt__], # occ
            [__deptran.flush_cnt__], # rococo
            #[__2pl.flush_cnt__], # 2pl timeout
            #[__2pl_wait_die.flush_cnt__], # 2pl wait die
            [__2pl_wound_die.flush_cnt__] # 2pl wound wait
            ]

    flush_sz = [
            [__occ.flush_sz__], # occ
            [__deptran.flush_sz__], # rococo
            #[__2pl.flush_sz__], # 2pl timeout
            #[__2pl_wait_die.flush_sz__], # 2pl wait die
            [__2pl_wound_die.flush_sz__] # 2pl wound wait
            ]

    throughput_val = [
            [__occ.tps__], # occ
            [__deptran.tps__], # rococo
            #[__2pl.tps__], # 2pl timeout
            #[__2pl_wait_die.tps__], # 2pl wait die
            [__2pl_wound_die.tps__] # 2pl wound wait
            ]

    commit_rate_val = [
            [__occ.commit_rate__],
            [__deptran.commit_rate__],
            #[__2pl.commit_rate__],
            #[__2pl_wait_die.commit_rate__],
            [__2pl_wound_die.commit_rate__]
            ]

    latency_50_val = [
            [__occ.latency_50__],
            [__deptran.latency_50__],
            #[__2pl.latency_50__],
            #[__2pl_wait_die.latency_50__],
            [__2pl_wound_die.latency_50__]
            ]

    latency_90_val = [
            [__occ.latency_90__],
            [__deptran.latency_90__],
            #[__2pl.latency_90__],
            #[__2pl_wait_die.latency_90__],
            [__2pl_wound_die.latency_90__]
            ]

    latency_99_val = [
            [__occ.latency_99__],
            [__deptran.latency_99__],
            #[__2pl.latency_99__],
            #[__2pl_wait_die.latency_99__],
            [__2pl_wound_die.latency_99__]
            ]

    latency_999_val = [
            [__occ.latency_999__],
            [__deptran.latency_999__],
            #[__2pl.latency_999__],
            #[__2pl_wait_die.latency_999__],
            [__2pl_wound_die.latency_999__]
            ]

    n_try_50_val = [
            [__occ.n_try_50__],
            [__deptran.n_try_50__],
            #[__2pl.n_try_50__],
            #[__2pl_wait_die.n_try_50__],
            [__2pl_wound_die.n_try_50__]
            ]

    n_try_90_val = [
            [__occ.n_try_90__],
            [__deptran.n_try_90__],
            #[__2pl.n_try_90__],
            #[__2pl_wait_die.n_try_90__],
            [__2pl_wound_die.n_try_90__]
            ]

    n_try_99_val = [
            [__occ.n_try_99__],
            [__deptran.n_try_99__],
            #[__2pl.n_try_99__],
            #[__2pl_wait_die.n_try_99__],
            [__2pl_wound_die.n_try_99__]
            ]

    n_try_999_val = [
            [__occ.n_try_999__],
            [__deptran.n_try_999__],
            #[__2pl.n_try_999__],
            #[__2pl_wait_die.n_try_999__],
            [__2pl_wound_die.n_try_999__]
            ]

    att_latency_50_val = [
            [__occ.att_latency_50__],
            [__deptran.att_latency_50__],
            #[__2pl.att_latency_50__],
            #[__2pl_wait_die.att_latency_50__],
            [__2pl_wound_die.att_latency_50__]
            ]

    att_latency_90_val = [
            [__occ.att_latency_90__],
            [__deptran.att_latency_90__],
            #[__2pl.att_latency_90__],
            #[__2pl_wait_die.att_latency_90__],
            [__2pl_wound_die.att_latency_90__]
            ]

    att_latency_99_val = [
            [__occ.att_latency_99__],
            [__deptran.att_latency_99__],
            #[__2pl.att_latency_99__],
            #[__2pl_wait_die.att_latency_99__],
            [__2pl_wound_die.att_latency_99__]
            ]

    att_latency_999_val = [
            [__occ.att_latency_999__],
            [__deptran.att_latency_999__],
            #[__2pl.att_latency_999__],
            #[__2pl_wait_die.att_latency_999__],
            [__2pl_wound_die.att_latency_999__]
            ]

    latency_min_val = [
            [__occ.min_latency__],
            [__deptran.min_latency__],
            #[__2pl.min_latency__],
            #[__2pl_wait_die.min_latency__],
            [__2pl_wound_die.min_latency__]
            ]

    latency_max_val = [
            [__occ.max_latency__],
            [__deptran.max_latency__],
            #[__2pl.max_latency__],
            #[__2pl_wait_die.max_latency__],
            [__2pl_wound_die.max_latency__]
            ]

    attempts_val = [
            [__occ.attempts__],
            [__deptran.attempts__],
            #[__2pl.attempts__],
            #[__2pl_wait_die.attempts__],
            [__2pl_wound_die.attempts__]
            ]

    graph_size_val = [
            [__deptran.graph_size__]
            ]

    n_ask_val = [
            [__deptran.n_ask__]
            ]

    scc_val = [
            [__deptran.scc__]
            ]

    throughput_figname =            "fig/__BENCH_NAME___ct_tp.pdf"
    tpcc_ct_cpu_figname =           "fig/__BENCH_NAME___ct_cpu.pdf"

    figure.tpcc_sc_tp(throughput_val, throughput_figname);
    figure.tpcc_sc_cpu(cpu, tpcc_ct_cpu_figname);

#    print("Hello")
