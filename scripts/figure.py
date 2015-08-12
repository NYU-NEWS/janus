#! /usr/bin/env python
# -*- coding: utf-8 -*-


xxx = [1, 2, 4, 8, 16, 32]

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import FuncFormatter

STR_CONCURRENT_REQS_PER_SERVER = "Concurrent reqs/server"
STR_THROUGHPUT = "Throughput"
STR_NUMBER_OF_SERVERS = "Number of servers"
STR_THROUGHPUT_NEW_ORDER = "Throughput (New-order/s)"
STR_CPU_UTILIZATION = "CPU Utilization"
STR_LATENCY_MS = "Latency(ms)"
STR_ATTEMPT_NEW_ORDER = "Attempts(new-order/s)"
STR_NUMBER_OF_TRIES_PER_COMMIT = "Number of tries per commit"
STR_LATENCY_MS_IN_LOG_SCALE = "Latency(ms) in log scale"
STR_COMMIT_RATE = "commit rate"


mpl.rcParams['font.sans-serif'] = ['SimHei']
STR_CONCURRENT_REQS_PER_SERVER = u"并发请求数/服务器"
STR_THROUGHPUT = u"吞吐量"
STR_NUMBER_OF_SERVERS = u"服务器数量"
STR_THROUGHPUT_NEW_ORDER = u"吞吐量（New-order/s）"
STR_CPU_UTILIZATION = u"CPU使用率"
STR_LATENCY_MS = u"延迟（ms）"
STR_ATTEMPT_NEW_ORDER = u"尝试次数（New-order/s）"
STR_NUMBER_OF_TRIES_PER_COMMIT = u"每次成功提交需要尝试次数"
STR_LATENCY_MS_IN_LOG_SCALE = u"延迟（ms）"
STR_COMMIT_RATE = u"提交成功率"


ROCOCO = "Rococo"
ROCOCO = "DepTran"

SHOW = True
SHOW = False
X_LOG_SCALE = False


eb_dis = 0.3
fig_scale = 5.0/8
#mpl.rcParams['xtick.direction'] = 'out'
#mpl.rcParams['ytick.direction'] = 'out'
mpl.rcParams['font.size'] =         12.5
mpl.rcParams['legend.fontsize'] =   12.5
mpl.rcParams['lines.linewidth'] =   3
mpl.rcParams['lines.markersize'] =  14 * fig_scale
mpl.rcParams['axes.grid'] =  True

#txt_legends = ["OCC", ROCOCO, "2PL-T", "2PL-WD", "2PL-WW"]
txt_legends = ["OCC", ROCOCO, "2PL", "RO6"]
line_styles = ["v-", "cx--", "mo:", "kp-.", "yd-."]
colors = ['#F84E1A', 'black', '#1B77F9', '#535353', '#A2DCFD','black', 'blue', 'green', 'red', 'black',"cyan", 'magenta', 'yellow']

bar_colors = [
                '#A2DCFD', '#1B77F9', 'blue',    # blue
                '#FFA07A', '#DC143C', '#8B0000', # red
                '#DCDCDC', '#696969', 'black',
                '#98FB98', '#3CB371', '#006400', # green
                '#535353', 'black',  'green',
                'red', 'black', "cyan",
                'magenta', 'yellow']

#bar_hatches = ["OOO", "///", "\\\\\\", "...", "xxx", "ooo", "\\|\\", "***", "/|/", "|||",]
bar_hatches = [

                "///", "///", "///",
                "", "", "",
                "\\\\\\", "\\\\\\", "\\\\\\",
                "...", "xxx", "ooo",
                "\\|\\", "***", "/|/", "|||",]

def flat_log(x, pos):
    return '%d' % x

def sort_legend(ax, ys):
    order = range(0, len(ys))
    sum_list = [sum(points) for points in ys]
    i = 0
    while (i < len(ys) - 1):
        j = i
        while (j < len(ys) - 1):
            if (sum_list[j] < sum_list[j + 1]):
                tmp = sum_list[j]
                sum_list[j] = sum_list[j + 1]
                sum_list[j + 1] = tmp
                tmp2 = order[j]
                order[j] = order[j + 1]
                order[j + 1] = tmp2
            j += 1
        i += 1
    handles, labels = ax.get_legend_handles_labels()
    new_handles = []
    new_labels = []
    for index in order:
        new_handles.append(handles[index])
        new_labels.append(labels[index])
    return new_handles, new_labels

def micro_tp(xs, ys, figname):
    fig, ax = plt.subplots(figsize=(12 * fig_scale, 25.0 / 4 * fig_scale))
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.xaxis.set_ticks_position('bottom')
    ax.yaxis.set_ticks_position('left')

    width = 0.4 / fig_scale
    ind = np.arange(len(xs))

    #legends=[   ["OCC 50%", "OCC 90%", "OCC 99%"],
    #            ["DepTran 50%", "DepTran 90%", "DepTran 99%"],
    #            ["2PL-P 50%", "2PL-P 90%", "2PL-P 99%"]]

    #ax.bar(ind, ys, width, color = '#1B77F9')
    ax.bar(ind, ys, width, color = 'black')

    plt.xlim(-width/2,  len(xs))
    ys_max = 0.0
    for k in ys:
        if ys_max < k:
            ys_max = k
    plt.ylim(0, ys_max * 1.2)
    #plt.xticks(xs+3, xs)
    xs=['1 RPC', ' 1 RPC\n+1 DB', ' 3 RPC\n+ 3 DB', 'OCC', '2PL', ROCOCO]

    #plt.legend(ncol=3, loc="upper center", mode="expand", bbox_to_anchor=(0., 1.1, 1, 0.1))
    #plt.xlabel("")
    plt.ylabel(STR_THROUGHPUT)
    ax.set_xticks(ind +width/2)
    plt.setp(ax.set_xticklabels(xs), fontsize=14)

    plt.savefig(figname, bbox_inches="tight")
    if SHOW: plt.show()
    pass

def tpcc_sc_tp(val, figname):
    fig, ax = plt.subplots(figsize=(8 * fig_scale, 5 * fig_scale))
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.xaxis.set_ticks_position('bottom')
    ax.yaxis.set_ticks_position('left')

    #val_occ = [ v1/(v2+0.0) for v1, v2 in zip(val[0], val[1])]
    #val_2pl = [ v1/(v2+0.0) for v1, v2 in zip(val[2], val[1])]
    #plt.plot(xxx, val_occ, line_styles[0], label=txt_legends[0], color=colors[0])
    #plt.plot(xxx, val_2pl, line_styles[2], label=txt_legends[2], color=colors[0])

    for i in range(0, len(val)):
        plt.plot(xxx, val[i], line_styles[i], label=txt_legends[i], color=colors[i])

    handles, labels = sort_legend(ax, val)
    plt.legend(handles, labels, ncol=1, loc="best")
    plt.xlabel(STR_NUMBER_OF_SERVERS)
    plt.ylabel(STR_THROUGHPUT_NEW_ORDER)
#    plt.ylim(0,400)
    #plt.xticks(np.arange(len(txt_sizes)), txt_sizes)
    plt.savefig(figname, bbox_inches="tight")
    if SHOW: plt.show()

def tpcc_sc_cpu(val, figname):
    fig, ax = plt.subplots(figsize=(8 * fig_scale, 5 * fig_scale))
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.xaxis.set_ticks_position('bottom')
    ax.yaxis.set_ticks_position('left')

    #val_occ = [ v1/(v2+0.0) for v1, v2 in zip(val[0], val[1])]
    #val_2pl = [ v1/(v2+0.0) for v1, v2 in zip(val[2], val[1])]
    #plt.plot(xxx, val_occ, line_styles[0], label=txt_legends[0], color=colors[0])
    #plt.plot(xxx, val_2pl, line_styles[2], label=txt_legends[2], color=colors[0])

    for i in range(0, len(val)):
        plt.plot(xxx, val[i], line_styles[i], label=txt_legends[i], color=colors[i])

    handles, labels = sort_legend(ax, val)
    plt.legend(handles, labels, ncol=1, loc="best")
    plt.xlabel(STR_NUMBER_OF_SERVERS)
    plt.ylabel(STR_CPU_UTILIZATION)
#    plt.ylim(0,400)
    #plt.xticks(np.arange(len(txt_sizes)), txt_sizes)
    plt.savefig(figname, bbox_inches="tight")
    if SHOW: plt.show()

def tpcc_ct_tp(val, figname):
    fig, ax = plt.subplots(figsize=(8 * fig_scale, 5 * fig_scale))
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.xaxis.set_ticks_position('bottom')
    ax.yaxis.set_ticks_position('left')

    if X_LOG_SCALE: ax.set_xscale('log')

    #val_occ = [ v1/(v2+0.0) for v1, v2 in zip(val[0], val[1])]
    #val_2pl = [ v1/(v2+0.0) for v1, v2 in zip(val[2], val[1])]
    #plt.plot(xxx, val_occ, line_styles[0], label=txt_legends[0], color=colors[0])
    #plt.plot(xxx, val_2pl, line_styles[2], label=txt_legends[2], color=colors[0])

    for i in range(0, len(val)):
        plt.plot(xxx, val[i], line_styles[i], label=txt_legends[i], color=colors[i])

    handles, labels = sort_legend(ax, val)
    plt.legend(handles, labels, ncol=1, loc="best")
    plt.xlabel(STR_CONCURRENT_REQS_PER_SERVER)
    plt.ylabel(STR_THROUGHPUT_NEW_ORDER)
#    plt.ylim(0,400)
    #plt.xticks(np.arange(len(txt_sizes)), txt_sizes)
    plt.savefig(figname, bbox_inches="tight")
    if SHOW: plt.show()

def tpcc_ct_cpu(val, figname):
    fig, ax = plt.subplots(figsize=(8 * fig_scale, 5 * fig_scale))
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.xaxis.set_ticks_position('bottom')
    ax.yaxis.set_ticks_position('left')

    #val_occ = [ v1/(v2+0.0) for v1, v2 in zip(val[0], val[1])]
    #val_2pl = [ v1/(v2+0.0) for v1, v2 in zip(val[2], val[1])]
    #plt.plot(xxx, val_occ, line_styles[0], label=txt_legends[0], color=colors[0])
    #plt.plot(xxx, val_2pl, line_styles[2], label=txt_legends[2], color=colors[0])

    for i in range(0, len(val)):
        plt.plot(xxx, val[i], line_styles[i], label=txt_legends[i], color=colors[i])

    handles, labels = sort_legend(ax, val)
    plt.legend(handles, labels, ncol=1, loc="best")
    plt.xlabel(STR_CONCURRENT_REQS_PER_SERVER)
    plt.ylabel(STR_CPU_UTILIZATION)
#    plt.ylim(0,400)
    #plt.xticks(np.arange(len(txt_sizes)), txt_sizes)
    plt.savefig(figname, bbox_inches="tight")
    if SHOW: plt.show()

def tpcc_ct_nt_eb(val_50, val_90, val_99, figname):
    fig, ax = plt.subplots(figsize=(8 * fig_scale, 5 * fig_scale))
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.xaxis.set_ticks_position('none')
    ax.yaxis.set_ticks_position('left')
    # ax.set_yscale('log')

    xs = np.arange(1, 21, 1)
    # width = 2

    for i in range(0, len(val_50)):
        v50 = [v for x, v in zip(xxx, val_50[i]) if x <= 20]
        v90 = [v for x, v in zip(xxx, val_90[i]) if x <= 20]
        v99 = [v for x, v in zip(xxx, val_99[i]) if x <= 20]

        yerr1 = [(v1-v2) for v1, v2 in zip(v90, v50)]
        yerr2 = [(v1-v2) for v1, v2 in zip(v99, v90)]

        #plt.plot(v90, label=txt_legends[i])
        plt.errorbar(xs + i*eb_dis, v90, yerr=[yerr1, yerr2], label=txt_legends[i], elinewidth=3)

    plt.xlim(1, 21)
    #plt.ylim(0,10)
    #plt.xticks(xs+3, xs)

    #plt.legend(ncol=3, loc="upper center", mode="expand", bbox_to_anchor=(0., 1.1, 1, 0.1))
    handles, labels = sort_legend(ax, val_90)
    plt.legend(handles, labels, ncol=1, loc="best")
    plt.xlabel(STR_CONCURRENT_REQS_PER_SERVER)
    plt.ylabel(STR_NUMBER_OF_TRIES_PER_COMMIT)
    plt.savefig(figname, bbox_inches="tight")
    if SHOW: plt.show()
    pass

def tpcc_ct_lt_eb(val_50, val_90, val_99, figname):
    fig, ax = plt.subplots(figsize=(8 * fig_scale, 5 * fig_scale))
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.xaxis.set_ticks_position('none')
    ax.yaxis.set_ticks_position('left')
    ax.set_yscale('log')
    y_formatter = FuncFormatter(flat_log)
    ax.yaxis.set_major_formatter(y_formatter)
    #ax.set_xscale('log')

    maxx1 = 20
    maxx2 = 100
    xs = np.arange(1, maxx1+1, 1)
    tmp = np.arange(maxx1+10, maxx2+1, 10)
    xs = np.concatenate((xs, tmp))

    # width = 2

    for i in range(0, len(val_50)):
        v50 = [v for x, v in zip(xxx, val_50[i]) if x <= maxx2]
        v90 = [v for x, v in zip(xxx, val_90[i]) if x <= maxx2]
        v99 = [v for x, v in zip(xxx, val_99[i]) if x <= maxx2]

        yerr1 = [(v1-v2) for v1, v2 in zip(v90, v50)]
        yerr2 = [(v1-v2) for v1, v2 in zip(v99, v90)]

        #print(len(xs))
        #print(len(v90))
        #plt.plot(v90, label=txt_legends[i])
        plt.errorbar(xs + i*eb_dis, v90, yerr=[yerr1, yerr2], label=txt_legends[i], elinewidth=3, color=colors[i])

    plt.xlim(1, maxx2+1)
    #plt.ylim(0,10)
    #plt.xticks(xs+3, xs)

    #plt.legend(ncol=3, loc="upper center", mode="expand", bbox_to_anchor=(0., 1.1, 1, 0.1))
    handles, labels = sort_legend(ax, val_90)
    plt.legend(handles, labels, ncol=1, loc="best")
    plt.xlabel(STR_CONCURRENT_REQS_PER_SERVER)
    plt.ylabel(STR_LATENCY_MS)
    plt.savefig(figname, bbox_inches="tight")
    if SHOW: plt.show()
    pass

def tpcc_ct_lt_bar(val_min, val_50, val_90, val_99, figname):
    fig, ax = plt.subplots(figsize=(8 * fig_scale, 5 * fig_scale))
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.xaxis.set_ticks_position('none')
    ax.yaxis.set_ticks_position('left')
    ax.set_yscale('log')

    # 10, 20, 30, ... , to 100.
    xs = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100];
    xs = np.arange(10, 101, 10)
    width = 2

    legends=[   ["OCC 50%", "OCC 90%", "OCC 99%"],
                ["DepTran 50%", "DepTran 90%", "DepTran 99%"],
                ["2PL-P 50%", "2PL-P 90%", "2PL-P 99%"]]

    for i in range(0, len(val_50)):
        v50 = [v for x, v in zip(xxx, val_50[i]) if x % 10 == 0]
        v90 = [v for x, v in zip(xxx, val_90[i]) if x % 10 == 0]
        v99 = [v for x, v in zip(xxx, val_99[i]) if x % 10 == 0]
        vmin = [v for x, v in zip(xxx, val_min[i]) if x % 10 == 0]
        bottom = np.zeros(len(xs))

        yerr1 = [(v1-v2) for v1, v2 in zip(v50, vmin)]
        yerr2 = [0] * len(v50)

        plt.bar(xs+i*width, v50, bottom=bottom, color=bar_colors[i*3+0], hatch=bar_hatches[i*3+0], width=width, log=True, label=legends[i][0], yerr=[yerr1, yerr2])
        bottom+=v50
        plt.bar(xs+i*width, v90, bottom=bottom, color=bar_colors[i*3+1], hatch=bar_hatches[i*3+1], width=width, log=True, label=legends[i][1])
        bottom+=v90
        plt.bar(xs+i*width, v99, bottom=bottom, color=bar_colors[i*3+2], hatch=bar_hatches[i*3+2], width=width, log=True, label=legends[i][2])
        bottom+=v99

    plt.xlim(9,110)
    plt.ylim(0,10)
    plt.xticks(xs+3, xs)

    plt.legend(ncol=3, loc="upper center", mode="expand", bbox_to_anchor=(0., 1.1, 1, 0.1))
    plt.xlabel(STR_CONCURRENT_REQS_PER_SERVER)
    plt.ylabel(STR_LATENCY_MS_IN_LOG_SCALE)
    plt.savefig(figname, bbox_inches="tight")
    if SHOW: plt.show()
    pass

def tpcc_ct_nt(val, figname):

#    txt_sizes = ["1K", "4K", "16K", "64K", "256K", "1M", "4M", "16M"]
    fig, ax = plt.subplots(figsize=(8 * fig_scale, 5 * fig_scale))
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.xaxis.set_ticks_position('bottom')
    ax.yaxis.set_ticks_position('left')

    for i in range(0, len(val)):
        plt.plot(xxx, val[i], line_styles[i], label=txt_legends[i], color=colors[i])

    handles, labels = sort_legend(ax, val)
    plt.legend(handles, labels, ncol=1, loc="upper left")
    plt.xlabel(STR_CONCURRENT_REQS_PER_SERVER)
    plt.ylabel(STR_NUMBER_OF_TRIES_PER_COMMIT)
#    plt.ylim(0,400)
    #plt.xticks(np.arange(len(txt_sizes)), txt_sizes)
    plt.savefig(figname, bbox_inches="tight")
    if SHOW: plt.show()

def tpcc_ct_lt(val, figname):

#    txt_sizes = ["1K", "4K", "16K", "64K", "256K", "1M", "4M", "16M"]
    fig, ax = plt.subplots(figsize=(8 * fig_scale, 5 * fig_scale))
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.xaxis.set_ticks_position('bottom')
    ax.yaxis.set_ticks_position('left')

    for i in range(0, len(val)):
        plt.plot(xxx, val[i], line_styles[i], label=txt_legends[i], color=colors[i])

    handles, labels = sort_legend(ax, val)
    plt.legend(handles, labels, ncol=1, loc="upper left")
    plt.xlabel(STR_CONCURRENT_REQS_PER_SERVER)
    plt.ylabel(STR_LATENCY_MS)
#    plt.ylim(0,400)
    #plt.xticks(np.arange(len(txt_sizes)), txt_sizes)
    plt.savefig(figname, bbox_inches="tight")
    if SHOW: plt.show()

def tpcc_ct_at(val, figname):

    fig, ax = plt.subplots(figsize=(8 * fig_scale, 5 * fig_scale))
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.xaxis.set_ticks_position('bottom')
    ax.yaxis.set_ticks_position('left')

    for i in range(0, len(val)):
        plt.plot(xxx, val[i], line_styles[i], label=txt_legends[i], color=colors[i])

    handles, labels = sort_legend(ax, val)
    plt.legend(handles, labels, ncol=1, loc="upper left")
    plt.xlabel(STR_CONCURRENT_REQS_PER_SERVER)
    plt.ylabel(STR_ATTEMPT_NEW_ORDER)
#    plt.ylim(0,400)
    #plt.xticks(np.arange(len(txt_sizes)), txt_sizes)
    plt.savefig(figname, bbox_inches="tight")
    if SHOW: plt.show()

def tpcc_ct_cr(val, figname):

    fig, ax = plt.subplots(figsize=(8 * fig_scale, 5 * fig_scale))
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.xaxis.set_ticks_position('bottom')
    ax.yaxis.set_ticks_position('left')

    if X_LOG_SCALE: ax.set_xscale('log')

    for i in range(0, len(val)):
        plt.plot(xxx, val[i], line_styles[i], label=txt_legends[i], color=colors[i])

    handles, labels = sort_legend(ax, val)
    plt.legend(handles, labels, ncol=1, loc="best")
    plt.xlabel(STR_CONCURRENT_REQS_PER_SERVER)
    plt.ylabel(STR_COMMIT_RATE)
    plt.ylim(0,1.2)
    #plt.xticks(np.arange(len(txt_sizes)), txt_sizes)
    plt.savefig(figname, bbox_inches="tight")
    if SHOW: plt.show()
