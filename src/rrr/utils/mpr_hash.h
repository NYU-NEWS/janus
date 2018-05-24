/* 
 * File:   mpr_hash.h
 * Author: ms
 * 
 * A thread-safe hash table that manage its key and value for itself.
 *
 * Created on October 2, 2013, 6:28 PM
 */

#ifndef MPR_HASH_H
#define	MPR_HASH_H

#include <stdlib.h>
#include <apr_hash.h>
#include <apr_thread_rwlock.h>
#include "logger.h"

#ifdef	__cplusplus
//extern "C" {
#endif

#define MPR_HASH_THREAD_UNSAFE  (0x0)
#define MPR_HASH_THREAD_SAFE    (0x1)


typedef struct {
    apr_pool_t *mp;
    apr_thread_mutex_t *mx;
    apr_thread_rwlock_t *rwl;
    apr_hash_t *ht;

    int is_thread_safe;
} mpr_hash_t;    

typedef struct {
    void *key;
    size_t sz_key;
    void *value;
    size_t sz_value;
} mpr_hash_value_t;


static void mpr_hash_create_ex(mpr_hash_t **hash, int flag) {
    *hash = (mpr_hash_t*)calloc(sizeof(mpr_hash_t), 1);
    apr_initialize();
    apr_pool_create(&(*hash)->mp, NULL);
    (*hash)->ht = apr_hash_make((*hash)->mp);
    apr_thread_mutex_create(&(*hash)->mx, APR_THREAD_MUTEX_UNNESTED, (*hash)->mp);
    apr_thread_rwlock_create(&(*hash)->rwl, (*hash)->mp);
    (*hash)->is_thread_safe = flag & MPR_HASH_THREAD_SAFE;
}

static void mpr_hash_create(mpr_hash_t **hash) {
    mpr_hash_create_ex(hash, MPR_HASH_THREAD_SAFE); 
}

static void mpr_hash_destroy(mpr_hash_t *hash) {
    // TODO [fix] remove all keys.
    
    apr_hash_index_t *hi;
    for (hi = apr_hash_first(NULL, hash->ht); hi; 
            hi = apr_hash_next(hi)) {
        void *k = NULL;
        mpr_hash_value_t *v = NULL;
        apr_hash_this(hi, (const void **)&k, NULL, (void **)&v);
        free(v->key);
        free(v->value);
        free(v);
    }
    
//    apr_thread_mutex_destroy(hash->mx);
    apr_thread_rwlock_destroy(hash->rwl);
    apr_pool_destroy(hash->mp);
    free(hash);
    atexit(apr_terminate);
}

static void mpr_hash_set(mpr_hash_t *hash, const void *key, size_t sz_key, 
        const void *value, size_t sz_value) {
//    apr_thread_mutex_lock(hash->mx);
    apr_thread_rwlock_wrlock(hash->rwl);    
    // delete the old value.
    mpr_hash_value_t *v_old = (mpr_hash_value_t*) apr_hash_get(hash->ht, key, sz_key);
    if (v_old != NULL) {
        apr_hash_set(hash->ht, v_old->key, v_old->sz_key, NULL);
        free(v_old->key);
        free(v_old->value);
        free(v_old);
    }
    
    
    mpr_hash_value_t *v_new = NULL;
    if (value != NULL) {
        LOG_TRACE("set new value in hash table.");
        v_new = (mpr_hash_value_t*) malloc(sizeof(mpr_hash_value_t));
        v_new->key = malloc(sz_key);
        v_new->value = malloc(sz_value);
        memcpy(v_new->key, key, sz_key);
        memcpy(v_new->value, value, sz_value);
        v_new->sz_key = sz_key;
        v_new->sz_value = sz_value;
        apr_hash_set(hash->ht, v_new->key, v_new->sz_key, v_new);
    } else {
        // delete the value.
    }
    
    //apr_thread_mutex_unlock(hash->mx);
    apr_thread_rwlock_unlock(hash->rwl);
}

static void mpr_hash_get(mpr_hash_t *hash, const void *key, size_t sz_key, 
        void **value, size_t *sz_value) {
//    apr_thread_mutex_lock(hash->mx);
    if (hash->is_thread_safe) {
        apr_thread_rwlock_rdlock(hash->rwl);
    }
    
    mpr_hash_value_t *v_old = (mpr_hash_value_t*)apr_hash_get(hash->ht, key, sz_key);
    if (v_old != NULL) {
        *value = v_old->value;
        *sz_value = v_old->sz_value;
    } else {
        *value = NULL;
        *sz_value = 0;
    }
    
    if (hash->is_thread_safe) {
        apr_thread_rwlock_unlock(hash->rwl);    
    }
//    apr_thread_mutex_unlock(hash->mx);    
}


#ifdef	__cplusplus
//}
#endif

#endif	/* MPR_HASH_H */

