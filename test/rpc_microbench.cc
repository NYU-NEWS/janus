#include <string>
#include <vector>
#include <algorithm>
#include <atomic>

#include <stdio.h>
#include <stdlib.h>
#include <semaphore.h>
#include <unistd.h>
#include <sys/time.h>
#include <signal.h>
#include <math.h>

#include "rrr.hpp"
#include "benchmark_service.h"

using namespace benchmark;
using namespace rrr;
using namespace std;

//const char *svr_addr = "127.0.0.1:8848";
//int byte_size = 10;
//int epoll_instances = 2;
//bool fast_requests = false;
//int seconds = 10;
//int client_threads = 8;
//int worker_threads = 16;
//int outgoing_requests = 1000;
//
//static string request_str;
//PollMgr *poll;
//PollMgr **polls;
//ThreadPool* thrpool;
//
//Counter req_counter;
//std::vector<std::vector<double> > *response;
//std::atomic<uint32_t> response_index;
//std::vector<uint64_t> qps;
//
//bool should_stop = false;
//
//pthread_mutex_t g_stop_mutex;
//pthread_cond_t g_stop_cond;
//
//static void signal_handler(int sig) {
//    Log_info("caught signal %d, stopping server now", sig);
//    should_stop = true;
//    Pthread_mutex_lock(&g_stop_mutex);
//    Pthread_cond_signal(&g_stop_cond);
//    Pthread_mutex_unlock(&g_stop_mutex);
//}
//
//static void* stat_proc(void*) {
//    i64 last_cnt = 0;
//    response_index.store(0);
//    for (int i = 0; i < seconds; i++) {
//        int cnt = req_counter.peek_next();
//        response_index++;
//        if (last_cnt != 0) {
//            qps[i] = cnt - last_cnt;
//            Log::info("qps: %ld", cnt - last_cnt);
//        } else {
//            qps[i] = 0;
//        }
//        last_cnt = cnt;
//        sleep(1);
//    }
//    should_stop = true;
//
//    int max_index = 0;
//    uint64_t max_qps = 0;
//    for (int i = seconds / 3; i < seconds * 2 / 3; i++) {
//        if (qps[i] > max_qps) {
//            max_index = i;
//            max_qps = qps[i];
//        }
//    }
//    std::vector<double> latency;
//    for (int i = 0; i < client_threads; i++)
//        latency.insert(latency.end(), response[i][max_index].begin(), response[i][max_index].end());
//    std::sort(latency.begin(), latency.end());
//    size_t l_length = latency.size();
//    double sum = 0.0;
//    int p50 = floor(l_length * 0.5);
//    int p90 = floor(l_length * 0.90);
//    int p99 = floor(l_length * 0.99);
//    int p999 = floor(l_length * 0.999);
//    double l50, l90, l99, l999;
//    int ii = 0;
//    for (; ii < p50; ii++)
//        sum += latency[ii];
//    l50 = sum / p50;
//    for (; ii < p90; ii++)
//        sum += latency[ii];
//    l90 = sum / p90;
//    for (; ii < p99; ii++)
//        sum += latency[ii];
//    l99 = sum / p99;
//    for (; ii < p999; ii++)
//        sum += latency[ii];
//    l999 = sum / p999;
//    printf("QPS: %lu; 50.0%% LATENCY: %lf; 90.0%% LATENCY: %lf; 99.0%% LATENCY: %lf; 99.9%% LATENCY: %lf\n", max_qps, l50 * 1000, l90 * 1000, l99 * 1000, l999 * 1000);
//    pthread_exit(nullptr);
//    return nullptr;
//}
//
//typedef struct {
//    uint32_t cid;
//} client_para_t;
//
//static void* client_proc(void *args) {
//    client_para_t *client_para = (client_para_t *)args;
//    uint32_t cid = client_para->cid;
//    polls[cid] = new PollMgr(1);
//    Client* cl = new Client(polls[cid]);
//    verify(cl->connect(svr_addr) == 0);
//    i32 rpc_id;
//    if (fast_requests) {
//        rpc_id = BenchmarkService::FAST_NOP;
//    } else {
//        rpc_id = BenchmarkService::NOP;
//    }
//    struct timespec last_time;
//    //base::Timer timer;
//    FutureAttr fu_attr;
//    auto do_work = [cl, &fu_attr, rpc_id, &last_time] {
//        if (!should_stop) {
//            clock_gettime(CLOCK_REALTIME, &last_time);
//            Future* fu = cl->begin_request(rpc_id, fu_attr);
//            *cl << request_str;
//            cl->end_request();
//            Future::safe_release(fu);
//            req_counter.next();
//        }
//    };
//    fu_attr.callback = [&do_work, &last_time, cid] (Future* fu) {
//        if (fu->get_error_code() != 0) {
//            return;
//        }
//        struct timespec t_buf;
//        clock_gettime(CLOCK_REALTIME, &t_buf);
//        //timer.stop();
//        response[cid][response_index.load()].push_back(t_buf.tv_sec - last_time.tv_sec + (t_buf.tv_nsec - last_time.tv_nsec) / 1000000000.0);
//        last_time = t_buf;
//        do_work();
//    };
//    do_work();
//    while (!should_stop) {
//        sleep(1);
//    }
//
//    cl->close_and_release();
//    polls[cid]->release();
//    pthread_exit(nullptr);
//    return nullptr;
//}
//
//int main(int argc, char **argv) {
//    signal(SIGPIPE, SIG_IGN);
//    signal(SIGHUP, SIG_IGN);
//    signal(SIGCHLD, SIG_IGN);
//
//    bool is_client = false, is_server = false;
//
//    if (argc < 2) {
//        printf("usage: perftest OPTIONS\n");
//        printf("                -c|-s ip:port\n");
//        printf("                -b    byte_size\n");
//        printf("                -e    epoll_instances\n");
//        printf("                -f    fast_requests\n");
//        printf("                -n    seconds\n");
//        printf("                -o    outgoing_requests\n");
//        printf("                -t    client_threads\n");
//        printf("                -w    worker_threads\n");
//        exit(1);
//    }
//
//    char ch = 0;
//    while ((ch = getopt(argc, argv, "c:s:b:e:fn:o:t:w:"))!= -1) {
//        switch (ch) {
//        case 'c':
//            is_client = true;
//            svr_addr = optarg;
//            break;
//        case 's':
//            is_server = true;
//            svr_addr = optarg;
//            break;
//        case 'b':
//            byte_size = atoi(optarg);
//            break;
//        case 'e':
//            epoll_instances = atoi(optarg);
//            break;
//        case 'f':
//            fast_requests = true;
//            break;
//        case 'n':
//            seconds = atoi(optarg);
//            break;
//        case 'o':
//            outgoing_requests = atoi(optarg);
//            break;
//        case 't':
//            client_threads = atoi(optarg);
//            break;
//        case 'w':
//            worker_threads = atoi(optarg);
//            break;
//        default:
//            break;
//        }
//    }
//    verify(is_server || is_client);
//    if (is_server) {
//        Log::info("server will start at     %s", svr_addr);
//    } else {
//        Log::info("client will connect to   %s", svr_addr);
//    }
//    Log::info("packet byte size:        %d", byte_size);
//    Log::info("epoll instances:         %d", epoll_instances);
//    Log::info("fast reqeust:            %s", fast_requests ? "true" : "false");
//    Log::info("running seconds:         %d", seconds);
//    Log::info("outgoing requests:       %d", outgoing_requests);
//    Log::info("client threads:          %d", client_threads);
//    Log::info("worker threads:          %d", worker_threads);
//
//    request_str = string(byte_size, 'x');
//    thrpool = new ThreadPool(worker_threads);
//    if (is_server) {
//        poll = new PollMgr(epoll_instances);
//        BenchmarkService svc;
//        Server svr(poll, thrpool);
//        svr.reg(&svc);
//        verify(svr.start(svr_addr) == 0);
//
//        Pthread_mutex_init(&g_stop_mutex, nullptr);
//        Pthread_cond_init(&g_stop_cond, nullptr);
//
//        signal(SIGPIPE, SIG_IGN);
//        signal(SIGHUP, SIG_IGN);
//        signal(SIGCHLD, SIG_IGN);
//
//        signal(SIGALRM, signal_handler);
//        signal(SIGINT, signal_handler);
//        signal(SIGQUIT, signal_handler);
//        signal(SIGTERM, signal_handler);
//
//        Pthread_mutex_lock(&g_stop_mutex);
//        while (should_stop == false) {
//            Pthread_cond_wait(&g_stop_cond, &g_stop_mutex);
//        }
//        Pthread_mutex_unlock(&g_stop_mutex);
//
//        poll->release();
//
//    } else {
//        response = new std::vector<std::vector<double> >[client_threads];
//        qps.resize(seconds);
//        polls = (PollMgr **)malloc(sizeof(PollMgr *) * client_threads);
//        pthread_t* client_th = new pthread_t[client_threads];
//        client_para_t *client_para = (client_para_t *)malloc(client_threads * sizeof(client_para_t));
//        for (int i = 0; i < client_threads; i++) {
//            response[i].resize(seconds + 1);
//            client_para[i].cid = i;
//            Pthread_create(&client_th[i], nullptr, client_proc, (void *)(client_para + i));
//        }
//        pthread_t stat_th;
//        Pthread_create(&stat_th, nullptr, stat_proc, nullptr);
//        Pthread_join(stat_th, nullptr);
//        for (int i = 0; i < client_threads; i++) {
//            Pthread_join(client_th[i], nullptr);
//        }
//        delete [] response;
//        free(polls);
//        delete[] client_th;
//    }
//
//    thrpool->release();
//    return 0;
//}
