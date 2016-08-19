#include "../__dep__.h"
#include "../command.h"
#include "sched.h"
#include "exec.h"
#include "coord.h"

namespace rococo {

int TapirSched::OnDispatch(const vector<SimpleCommand> &cmds,
                           int32_t* res,
                           TxnOutput *output,
                           const function<void()> &callback) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto exec = GetOrCreateExecutor(cmds[0].root_id_);
  verify(exec->mdb_txn());
  exec->Execute(cmds, output);
  *res = SUCCESS;
  callback();
  return 0;

}

int TapirSched::OnFastAccept(cmdid_t cmd_id,
                             const vector<SimpleCommand>& txn_cmds,
                             int32_t* res,
                             const function<void()>& callback) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  Log_debug("receive fast accept for cmd_id: %llx", cmd_id);
  auto exec = (TapirExecutor*) GetOrCreateExecutor(cmd_id);
  exec->FastAccept(txn_cmds, res);

  // DEBUG
  verify(txn_cmds.size() > 0);
  for (auto& c: txn_cmds) {

  }
  callback();
  return 0;
}

int TapirSched::OnDecide(txnid_t txn_id,
                         int32_t decision,
                         const function<void()>& callback) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto exec = (TapirExecutor*) GetExecutor(txn_id);
  verify(exec);
  if (decision == TapirCoord::Decision::COMMIT) {
    exec->Commit();
  } else if (decision == TapirCoord::Decision::ABORT) {
    exec->Abort();
  } else {
    verify(0);
  }
  TrashExecutor(txn_id);
  callback();
}

} // namespace rococo