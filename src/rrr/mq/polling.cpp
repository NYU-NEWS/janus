#include "polling.h"


int poll_job_create(poll_job_t **job) {
    poll_job_t *j = (poll_job_t*) malloc(sizeof(poll_job_t));
    j->do_read = NULL;
    j->do_write = NULL;
    j->do_error = NULL;
    j->holder = NULL;
    j->mgr = NULL;
    j->worker= NULL;
    j->ps = NULL;
    *job = j;
    return 0;
}

void poll_job_destroy(poll_job_t *job) {
    free(job);
}

void* APR_THREAD_FUNC poll_worker_run(
        apr_thread_t *th,
        void *arg) {
    poll_worker_t *worker = (poll_worker_t*)arg;
    
    LOG_TRACE("poll worker loop starts");
    
    while (!worker->fg_exit) {
        int num = 0;
        const apr_pollfd_t *ret_pfd;
	// wait 100 * 1000 micro seconds.
        apr_status_t status = apr_pollset_poll(worker->ps, 100 * 1000, 
					       &num, &ret_pfd);

        if (status == APR_SUCCESS) {
   	    LOG_TRACE("poll worker loop: found something");
            SAFE_ASSERT(num > 0);
            for (int i = 0; i < num; i++) {
                int rtne = ret_pfd[i].rtnevents;
                SAFE_ASSERT(rtne & (APR_POLLIN | APR_POLLOUT));
                poll_job_t *job = (poll_job_t *)ret_pfd[i].client_data;
                SAFE_ASSERT(job != NULL);
                if (rtne & APR_POLLIN) {
	    	    LOG_TRACE("poll worker loop: found something to read");
		    SAFE_ASSERT(job->do_read != NULL);
		    (*job->do_read)(job->holder); 
                }
                if (rtne & APR_POLLOUT) {
	    	    LOG_TRACE("poll worker loop: found something to write");
		    SAFE_ASSERT(job->do_write);
                    (*job->do_write)(job->holder);
                }
            }
        } else if (status == APR_EINTR) {
            // the signal we get when process exit, 
            // wakeup, or add in and write.
            LOG_WARN("the receiver epoll exits?");
            continue;
        } else if (status == APR_TIMEUP) {
	    //LOG_DEBUG("poll time out");
            continue;
        } else {
            LOG_ERROR("unknown poll error. %s", 
		      apr_strerror(status, (char*)malloc(100), 100));
            SAFE_ASSERT(0);
        }
    }
    
    LOG_TRACE("poll worker loop stops");
    SAFE_ASSERT(apr_thread_exit(th, APR_SUCCESS) == APR_SUCCESS);
    return NULL;
}

int poll_worker_start(poll_worker_t *worker) {
    apr_thread_create(&worker->th_poll, NULL, 
            poll_worker_run, (void*)worker, worker->mp_poll);  
    return 0;
}

int poll_worker_stop(poll_worker_t *worker) {
    apr_status_t status = 0;
    apr_thread_join(&status, worker->th_poll);
    return 0;
}

int poll_worker_create(
        poll_worker_t **worker,
        poll_mgr_t *mgr) {
    poll_worker_t *w = (poll_worker_t*) malloc(sizeof(poll_worker_t));
    w->mgr = mgr;
    apr_pool_create(&w->mp_poll, NULL);
    w->ht_jobs = apr_hash_make(w->mp_poll);
    apr_thread_mutex_create(&w->mx_poll, APR_THREAD_MUTEX_UNNESTED, w->mp_poll);
    apr_pollset_create(&w->ps, 1000, w->mp_poll, APR_POLLSET_THREADSAFE);
    w->fg_exit = false;
    poll_worker_start(w);
    *worker = w;
    return 0;
}

int poll_worker_destroy(
        poll_worker_t *worker) {
    worker->fg_exit = true; 
    poll_worker_stop(worker);    
    apr_pollset_destroy(worker->ps);
    apr_pool_destroy(worker->mp_poll);
    free(worker);
    return 0;
}


/**
 * thread-safe on different jobs at the same worker.
 */
int poll_worker_add_job(
        poll_worker_t *worker,
        poll_job_t *job) {
    SAFE_ASSERT(worker != NULL);
    SAFE_ASSERT(job->ps == NULL);
    SAFE_ASSERT(apr_pollset_add(worker->ps, &job->pfd) == APR_SUCCESS);  
    job->ps = worker->ps;
    return 0;
}

int poll_worker_remove_job(
        poll_worker_t *worker,
        poll_job_t *job) {
    //    SAFE_ASSERT(job->ps != NULL);
    if (job->ps == NULL) {
	return -1;
    }
    
    SAFE_ASSERT(worker != NULL);
    SAFE_ASSERT(apr_pollset_remove(worker->ps, &job->pfd) == APR_SUCCESS);
    job->ps = NULL;
    return 0;
}

int poll_worker_update_job(
        poll_worker_t *worker,
        poll_job_t *job,
        int mode) {
    if (job->pfd.reqevents == mode) {
	return 0;
    }

    //    SAFE_ASSERT(job->ps != NULL);
    if(job->ps == NULL) {
	return -1;
    }

    SAFE_ASSERT(worker != NULL);

    apr_status_t status = 0;
    status = apr_pollset_remove(worker->ps, &job->pfd);   
    if (status != APR_SUCCESS) {
	LOG_ERROR("error in remove from pollset. code: %d, message: %s",
		  (int)status, apr_strerror(status, (char*)malloc(100), 100));
	SAFE_ASSERT(0);
    }
    
    job->pfd.reqevents = mode;
    status = apr_pollset_add(worker->ps, &job->pfd);
    if (status != APR_SUCCESS) {
	LOG_ERROR("error in readd to pollset. code: %d, message: %s",
		  (int) status, apr_strerror(status, (char*)malloc(100), 100));
    }
    return 0;
}

int poll_mgr_create(
        poll_mgr_t **mgr,
        int n_worker) {
    poll_mgr_t *m;
    m = (poll_mgr_t*) malloc(sizeof(poll_mgr_t));
    m->idx_worker = 0;
    m->n_worker = n_worker;    
    m->workers = (poll_worker_t**) malloc(n_worker * sizeof(poll_mgr_t*)); 
    for (int i = 0; i < n_worker; i++) {
        poll_worker_create(&m->workers[i], m);
    }
    *mgr = m; 
    return 0;
}

int poll_mgr_destroy(
        poll_mgr_t *mgr) {
    for (int i = 0; i < mgr->n_worker; i++) {
        poll_worker_destroy(mgr->workers[i]);
    }
    free(mgr);    
    return 0;
}

/**
 * thread-safe?
 */
int poll_mgr_add_job(
        poll_mgr_t *mgr,
        poll_job_t *job) {
    poll_worker_t *worker = mgr->workers[mgr->idx_worker];    
    mgr->idx_worker = (mgr->idx_worker + 1) % mgr->n_worker;
    job->worker = worker;
    poll_worker_add_job(worker, job);
    LOG_TRACE("add poll job to manager");
    return 0;
}

int poll_mgr_remove_job(
        poll_mgr_t *mgr,
        poll_job_t *job) {
    poll_worker_t *worker = job->worker;
    SAFE_ASSERT(job->mgr == mgr);
    SAFE_ASSERT(job->worker != NULL);
    poll_worker_remove_job(worker, job);
    LOG_TRACE("remove poll job from manager");
    return 0;
}

int poll_mgr_update_job(
        poll_mgr_t *mgr,
        poll_job_t *job,
        int mode) {
    poll_worker_t *worker = job->worker;
    SAFE_ASSERT(job->mgr == mgr);
    SAFE_ASSERT(job->worker != NULL);

    poll_worker_update_job(worker, job, mode);
    LOG_TRACE("update poll job in manager");
    return 0;
}
