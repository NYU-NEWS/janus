
#include "executor.h"
#include "scheduler.h"
#include "txn_reg.h"
#include "procedure.h"

namespace janus {

Executor::Executor(txnid_t txn_id, TxLogServer *sched)
    : cmd_id_(txn_id), sched_(sched) {
  verify(sched != nullptr);
}

Executor::~Executor() {
  verify(dtxn_ != nullptr);
  sched_->DestroyTx(cmd_id_); // destroy workspace.
  dtxn_ = nullptr;
}

void Executor::Execute(const SimpleCommand &cmd,
                       rrr::i32 *res,
                       map<int32_t, Value> &output) {
  verify(output.size() == 0);
  *res = SUCCESS;
  TxnPieceDef &p = txn_reg_->get(cmd.root_type_, cmd.type_);
  const auto &handler = p.proc_handler_;
  handler(this,
          *dtxn_,
          const_cast<SimpleCommand &>(cmd),
          res,
          output);
  // for debug;
  for (auto &pair: output) {
    verify(pair.second.get_kind() != Value::UNKNOWN);
  }
}

void Executor::Execute(const vector<SimpleCommand> &cmds,
                       TxnOutput *output) {
  TxWorkspace ws;
  for (const SimpleCommand &c: cmds) {
    auto &cmd = const_cast<SimpleCommand &>(c);
    TxnPieceDef &p = txn_reg_->get(c.root_type_, c.type_);
    const auto &handler = p.proc_handler_;
    auto &m = (*output)[c.inn_id_];
    int res;
    cmd.input.Aggregate(ws);
    handler(this,
            *dtxn_,
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

mdb::Txn *Executor::mdb_txn() {
  if (mdb_txn_ != nullptr) return mdb_txn_;
  verify(sched_ != nullptr);
  mdb::Txn *txn = sched_->GetOrCreateMTxn(cmd_id_);
  verify(txn != nullptr);
  mdb_txn_ = txn;
  return mdb_txn_;
}

} // namespace janus
