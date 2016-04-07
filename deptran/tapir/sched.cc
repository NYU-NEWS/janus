#include "../__dep__.h"
#include "../command.h"
#include "sched.h"
#include "exec.h"
#include "coord.h"

namespace rococo {

int TapirSched::OnDispatch(const SimpleCommand &cmd,
                           int32_t* res,
                           map<int32_t, Value> *output,
                           const function<void()> &callback) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
//  auto exec = (TapirExecutor*) GetOrCreateExecutor(cmd.root_id_);
  auto exec = GetOrCreateExecutor(cmd.root_id_);
//  verify(0);
  exec->mdb_txn();
  exec->Execute(cmd, res, *output);
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
  callback();
  return 0;
}

int TapirSched::OnDecide(cmdid_t cmd_id,
                         int32_t decision,
                         const function<void()>& callback) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto exec = (TapirExecutor*) GetOrCreateExecutor(cmd_id);
  if (decision == TapirCoord::COMMIT) {
    exec->Commit();
  } else if (decision == TapirCoord::ABORT) {
    exec->Abort();
  } else {
    verify(0);
  }
  DestroyExecutor(cmd_id);
  callback();
}

} // namespace rococo