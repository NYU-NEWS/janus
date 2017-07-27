#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <apr_thread_proc.h>
#include <apr_network_io.h>
#include <apr_poll.h>
#include <errno.h>
#include <inttypes.h>
#include "rpc.h"
#include "utils/logger.h"
#include "utils/safe_assert.h"
#include "utils/mpr_thread_pool.h"
#include "utils/mpr_hash.h"

#include "rpc.h"

poll_mgr_t *mgr_ = NULL;

#define MAX_ON_READ_THREADS 1
#define POLLSET_NUM 100
#define SZ_POLLSETS 4

// for statistics
static apr_time_t time_start_ = 0;
static apr_time_t time_last_ = 0;
static apr_time_t time_curr_ = 0;
static uint64_t sz_data_ = 0;
static uint64_t sz_last_ = 0;
static uint32_t n_data_recv_ = 0;
static uint32_t sz_data_sent_ = 0;
static uint32_t sz_data_tosend_ = 0;
static uint32_t n_data_sent_ = 0;

static uint32_t init_ = 0;

void rpc_init() {
    apr_initialize();

    poll_mgr_create(&mgr_, 1);

    //    apr_pool_create(&mp_rpc_, NULL);
    //
    // = 0; i < SZ_POLLSETS; i++) {
    //llset_create(&pollsets_[i], POLLSET_NUM, mp_rpc_, APR_POLLSET_THREADSAFE);
    //read_create(&ths_poll_[i], NULL, start_poll, (void*)pollsets_[i], mp_rpc_);
    //
    //
}

void rpc_destroy() {
    poll_mgr_destroy(mgr_);
//    while (init_ == 0) {
//        // not started.
//    }
//    exit_ = 1;
//    LOG_DEBUG("recv server ends.");
///*
//*/
//    apr_status_t status = APR_SUCCESS;
////    apr_thread_join(&status, th_poll_);
//    for (int i = 0; i < SZ_POLLSETS; i++) {
//        apr_thread_join(&status, ths_poll_[i]);
//        apr_pollset_destroy(pollsets_[i]);
//    }
//
//    apr_pool_destroy(mp_rpc_);
    atexit(apr_terminate);
}

//oid stat_on_write(size_t sz) {
//   LOG_TRACE("sent data size: %d", sz);
//   sz_data_sent_ += sz;
///    apr_atomic_sub32(&sz_data_tosend_, sz);
//
//
//
//**
//* not thread-safe
//* @param sz
//*/
//oid stat_on_read(size_t sz) {
//   time_curr_ = apr_time_now();
//   time_last_ = (time_last_ == 0) ? time_curr_ : time_last_;
//*
//   if (time_start_ == 0) {
//       time_start_ = (time_start_ == 0) ? time_curr_: time_start_;
//       recv_last_time = recv_start_time;
//   }
///
//   sz_data_ += sz;
//   sz_last_ += sz;
//   apr_time_t period = time_curr_ - time_last_;
//   if (period > 1000000) {
//       //uint32_t n_push;
//       //uint32_t n_pop;
//       //mpaxos_async_push_pop_count(&n_push, &n_pop);
//       float speed = (double)sz_last_ / (1024 * 1024) / (period / 1000000.0);
//       //printf("%d messages %"PRIu64" bytes received. Speed: %.2f MB/s. "
//       //    "Total sent count: %d,  bytes:%d, left to send: %d",// n_push:%d, n_pop:%d\n", 
//       //    apr_atomic_read32(&n_data_recv_), 
//       //    sz_data_, 
//       //    speed, 
//       //    apr_atomic_read32(&n_data_sent_), 
//       //    apr_atomic_read32(&sz_data_sent_),
//       //    apr_atomic_read32(&sz_data_tosend_)); 
//       //    //n_push, n_pop);
//       time_last_ = time_curr_;
//       sz_last_ = 0;
//   }
//


