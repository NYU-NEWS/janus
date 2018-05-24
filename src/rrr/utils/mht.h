
#pragma once

#include <apr_hash.h>

typedef struct {
    apr_hash_t *ht_v;
    apr_pool_t *pl;
    pthread_mutex_t mx;
} index_t;

void mht_put(char* ht_name, void* key, size_t sz_key, void* value, size_t sz_value);

void* mht_get(char* ht_name, void* k, size_t sz_k);

index_t* mht_ht_init(char* ht_name);

void mht_ht_destroy(char* ht_name);
