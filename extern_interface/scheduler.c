#include "scheduler.h"

// test for c-cpp bridge
int hello_c() {
  return 0;
}


//// a sample implementation for "none" mode.
int32_t handle_conflicts(int64_t txn_id,
                         int32_t n_confict,
                         _c_conflict_id_t* conflicts) {
  // never pause.
  return 0;
}

int64_t poll_executable() {
  return 0;
}
