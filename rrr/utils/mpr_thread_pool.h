#pragma once

#include <apr_queue.h>
#include <apr_thread_proc.h>
#include <apr_thread_mutex.h>
#include <apr_atomic.h>

#define SZ_QUEUE 1000000
#define SZ_THREADPOOL 1

typedef struct {
    apr_pool_t *mp; 
    apr_queue_t *qu;
    apr_thread_mutex_t *mx;
    apr_thread_t **th;
    apr_thread_start_t func;
    apr_uint32_t is_exit;
} mpr_thread_pool_t;

static void* APR_THREAD_FUNC mpr_thread_pool_run(apr_thread_t *t, void* arg) {
    mpr_thread_pool_t *tp = (mpr_thread_pool_t*) arg;
    while (apr_atomic_read32(&tp->is_exit) == 0) {
        void *param;
        apr_status_t status = APR_SUCCESS;
        status = apr_queue_pop(tp->qu, (void**)&param);
        if (status == APR_SUCCESS) {
            
        } else if(status == APR_EOF) {
            break;
        } else if(status == APR_EINTR) {
            // try again.
            continue;
        } else {
            LOG_ERROR("queue pop not successful: %s", 
		      apr_strerror(status, (char*)malloc(100), 100));
            SAFE_ASSERT(0);
        }
        (*(tp->func))(NULL, param);
    }
    SAFE_ASSERT(apr_thread_exit(t, APR_SUCCESS) == APR_SUCCESS);
    return NULL;
}

static void mpr_thread_pool_create(mpr_thread_pool_t **tp, apr_thread_start_t func) {
    *tp = (mpr_thread_pool_t*) malloc(sizeof(mpr_thread_pool_t));
    apr_pool_create(&(*tp)->mp, NULL);
    apr_queue_create(&(*tp)->qu, SZ_QUEUE, (*tp)->mp);
    apr_thread_mutex_create(&(*tp)->mx, APR_THREAD_MUTEX_UNNESTED, (*tp)->mp);
    apr_atomic_set32(&(*tp)->is_exit, 0);
    (*tp)->func = func;
    (*tp)->th = (apr_thread_t**)malloc(sizeof(apr_thread_t*) * SZ_THREADPOOL);
    for (int i = 0; i < SZ_THREADPOOL; i++) {
        apr_thread_create(&(*tp)->th[i], NULL, mpr_thread_pool_run, *tp, (*tp)->mp);  
    }
}

static void mpr_thread_pool_destroy(mpr_thread_pool_t *tp) {
    apr_atomic_set32(&tp->is_exit, 1);
    apr_queue_term(tp->qu);
    for (int i = 0; i < SZ_THREADPOOL; i++) {
        apr_status_t status;
        apr_thread_t *t = tp->th[i];
        apr_thread_join(&status, t);
        SAFE_ASSERT(status == APR_SUCCESS);
    }
    apr_thread_mutex_destroy(tp->mx);        
    apr_pool_destroy(tp->mp);
    free(tp);
}



static void mpr_thread_pool_push(mpr_thread_pool_t *tp, void *params) {
    while (1) {
        apr_status_t status = apr_queue_push(tp->qu, params);
        if (status == APR_SUCCESS) {
            break;
        } else if (status == APR_EOF) {
            break;
        } else if (status == APR_EINTR) {
            LOG_WARN("push failed, try again");
            continue;
        } else {
            SAFE_ASSERT(0);
        }
     }
    
}

static int mpr_thread_pool_task_count(mpr_thread_pool_t *tp) {
    return apr_queue_size(tp->qu);
}
