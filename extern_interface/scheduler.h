#pragma once

#include "inttypes.h"

/**
 * This is a C interface for external concurrency control protocols.
 * @param txn_id the transaction id greater than 0
 * @param rw_flag 0 for read for read, 1 for write.
 * @return 0 for continue and 1 for pause.
 */

typedef struct {
  int32_t rw_flag;
  char* key;
  int32_t sz_key;
  void** ptr_mem;
} _c_conflict_id_t;

int hello_c();

int32_t handle_conflicts(
    int64_t txn_id,
    int32_t n_confict,
    _c_conflict_id_t* conflicts
);

/**
 *
 * @return the transaction id for next executable 0 for nothing.
 */
int64_t poll_executable();

