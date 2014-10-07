#ifndef POLLING_H
#define POLLING_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <errno.h>
#include <inttypes.h>
#include <stdbool.h>
#include <apr_thread_proc.h>
#include <apr_network_io.h>
#include <apr_poll.h>
#include <apr_hash.h>
#include "utils/logger.h"
#include "utils/safe_assert.h"
//#include "utils/mpr_thread_pool.h"
//#include "utils/mpr_hash.h"


typedef struct __poll_job poll_job_t;
typedef struct __poll_worker poll_worker_t;
typedef struct __poll_mgr poll_mgr_t;

/*
 * each job is a pollable. 
 */
struct __poll_job {
    void* holder;       // which server, client, or server_connection this job belongs to.
    //    poll_worker_t* worker;       // which worker is doing this poll job.
    poll_mgr_t *mgr;          // which pollmgr is managin this poll job.
    poll_worker_t *worker;    // which worker holds this job.
    apr_pollfd_t pfd;   // this is where to poll 
    apr_pollset_t *ps;  // this is the pollset, actually it also available via poll worker.
    void (*do_read)(void*); 
    void (*do_write)(void*);
    void (*do_error)(void*);
};

/*
 * each worker is a thread.
 */
struct __poll_worker {
    poll_mgr_t *mgr;
    apr_pool_t *mp_poll;
    apr_thread_t *th_poll; 
    apr_thread_mutex_t *mx_poll;
    apr_hash_t *ht_jobs;
    apr_pollset_t *ps;
    bool fg_exit;
};

/*
 * each poll manager is in charge of many
 * workers. it dispatch new jobs randomly
 * to each worker.
 */
struct __poll_mgr {
    poll_worker_t **workers; 
    int n_worker;
    int idx_worker;
};

int poll_worker_create(
        poll_worker_t **worker,
        poll_mgr_t *mgr);

int poll_worker_destroy(
        poll_worker_t *worker);

void* APR_THREAD_FUNC poll_worker_run(
        apr_thread_t *th,
        void* arg);

int poll_job_create(poll_job_t **job);

int poll_worker_start(
        poll_worker_t *worker);

int poll_worker_stop(
        poll_worker_t *worker);

int poll_worker_add_job(
        poll_worker_t *worker,
        poll_job_t *job); 

int poll_worker_remove_job(
        poll_worker_t *worker,
        poll_job_t *job);

int poll_worker_update_job(
        poll_worker_t *worker,
        poll_job_t *job,
        int mode);

int poll_mgr_create(
        poll_mgr_t **mgr, 
        int n_worker);

int poll_mgr_destroy(
        poll_mgr_t *mgr);

int poll_mgr_add_job(
        poll_mgr_t *mgr, 
        poll_job_t *job);

int poll_mgr_remove_job(
        poll_mgr_t *mgr, 
        poll_job_t *job);

int poll_mgr_update_job(
        poll_mgr_t *mgr, 
        poll_job_t *job,
        int mode);


#endif // POLLING_H
