
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

Executor::~Executor(){
  verify(mdb_txn_ == nullptr);
  verify(dtxn_ != nullptr);
  sched_->DestroyDTxn(cmd_id_);
  dtxn_ = nullptr;
}

} // namespace rococo