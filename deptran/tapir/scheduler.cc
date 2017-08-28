#include "../__dep__.h"
#include "../command.h"
#include "tx.h"
#include "scheduler.h"
#include "exec.h"
#include "coordinator.h"

namespace janus {

bool SchedulerTapir::OnDispatch(TxPieceData &piece_data,
                                TxnOutput &ret_output) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
//  auto exec = GetOrCreateExecutor(cmds[0].root_id_);
//  verify(exec->mdb_txn());
//  exec->Execute(cmds, output);
//  *res = SUCCESS;
//  callback();
//  return 0;
  return true;
}

bool SchedulerTapir::Guard(Tx &tx, Row *row, int col_id, bool write) {
  // do nothing.
}

int SchedulerTapir::OnFastAccept(txid_t tx_id,
                                 const vector<SimpleCommand> &txn_cmds) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  Log_debug("receive fast accept for cmd_id: %llx", tx_id);
  auto exec = (TapirExecutor *) GetOrCreateExecutor(tx_id);
  int r = exec->FastAccept(txn_cmds);

  // DEBUG
  verify(txn_cmds.size() > 0);
  for (auto &c: txn_cmds) {

  }
  return r;
}

int SchedulerTapir::OnDecide(txnid_t txn_id,
                             int32_t decision,
                             const function<void()> &callback) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto exec = (TapirExecutor *) GetExecutor(txn_id);
  verify(exec);
  if (decision == CoordinatorTapir::Decision::COMMIT) {
#ifdef CHECK_ISO
    MergeDeltas(exec->dtxn_->deltas_);
#endif
    exec->Commit();
  } else if (decision == CoordinatorTapir::Decision::ABORT) {
    exec->Abort();
  } else {
    verify(0);
  }
  TrashExecutor(txn_id);
  callback();
}

} // namespace janus