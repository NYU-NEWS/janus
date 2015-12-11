
#include "executor.h"
#include "scheduler.h"

namespace rococo {

Executor::Executor(cmdid_t cmd_id, Scheduler *sched)
    : cmd_id_(cmd_id), sched_(sched) {
  mdb::Txn *txn = nullptr;
  txn = sched->GetOrCreateMTxn(cmd_id);
  verify(txn != nullptr);
  mdb_txn_ = txn;
}

} // namespace rococo