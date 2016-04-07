
#include "executor.h"
#include "scheduler.h"
#include "txn_reg.h"

namespace rococo {

Executor::Executor(txnid_t txn_id, Scheduler *sched)
    : cmd_id_(txn_id), sched_(sched) {
  verify(sched != nullptr);
}

Executor::~Executor() {
//  verify(mdb_txn_ == nullptr);
  verify(dtxn_ != nullptr);
  sched_->DestroyDTxn(cmd_id_);
  dtxn_ = nullptr;
}

void Executor::Execute(const SimpleCommand &cmd,
                       rrr::i32 *res,
                       map<int32_t, Value> &output) {
  verify(output.size() == 0);
  *res = SUCCESS;
  txn_reg_->get(cmd).txn_handler(this,
                                 dtxn_,
                                 const_cast<SimpleCommand&>(cmd),
                                 res,
                                 output);
  // for debug;
  for (auto &pair: output) {
    verify(pair.second.get_kind() != Value::UNKNOWN);
  }
}

mdb::Txn* Executor::mdb_txn() {
  if (mdb_txn_ != nullptr) return mdb_txn_;
  verify(sched_ != nullptr);
  mdb::Txn *txn = sched_->GetOrCreateMTxn(cmd_id_);
  verify(txn != nullptr);
  mdb_txn_ = txn;
  return mdb_txn_;
}

} // namespace rococo