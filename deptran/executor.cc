
#include "executor.h"
#include "scheduler.h"
#include "txn_reg.h"
#include "txn_chopper.h"

namespace rococo {

Executor::Executor(txnid_t txn_id, Scheduler *sched)
    : cmd_id_(txn_id), sched_(sched) {
  verify(sched != nullptr);
}

Executor::~Executor() {
  verify(dtxn_ != nullptr);
  sched_->DestroyDTxn(cmd_id_); // destroy workspace.
  dtxn_ = nullptr;
}

void Executor::Execute(const SimpleCommand &cmd,
                       rrr::i32 *res,
                       map<int32_t, Value> &output) {
  verify(output.size() == 0);
  *res = SUCCESS;
  const auto &handler = txn_reg_->get(cmd).txn_handler;
  handler(this,
          dtxn_,
          const_cast<SimpleCommand&>(cmd),
          res,
          output);
  // for debug;
  for (auto &pair: output) {
    verify(pair.second.get_kind() != Value::UNKNOWN);
  }
}

void Executor::Execute(const vector<SimpleCommand>& cmds,
                       TxnOutput* output) {
  TxnWorkspace ws;
  for (const SimpleCommand& c: cmds) {
    auto& cmd = const_cast<SimpleCommand&>(c);
    const auto &handler = txn_reg_->get(c).txn_handler;
    auto& m = (*output)[c.inn_id_];
    int res;
    cmd.input.Aggregate(ws);
    handler(this,
            dtxn_,
            cmd,
            &res,
            m);
    ws.insert(m);
#ifdef DEBUG_CODE
    for (auto &pair: output) {
      verify(pair.second.get_kind() != Value::UNKNOWN);
    }
#endif
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