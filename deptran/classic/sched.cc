
#include "../constants.h"
#include "../dtxn.h"
#include "../txn_chopper.h"
#include "../coordinator.h"
#include "sched.h"
#include "../tpl/tpl.h"
#include "exec.h"

namespace rococo {

int ClassicSched::OnDispatch(const SimpleCommand &cmd,
                             int32_t *res,
                             map<int32_t, Value> *output,
                             const function<void()>& callback) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto exec = (ClassicExecutor*) GetOrCreateExecutor(cmd.root_id_);
  exec->StartLaunch(cmd,
                    res,
                    output,
                    callback);
  return 0;
}

int ClassicSched::OnPrepare(cmdid_t cmd_id,
                            const std::vector<i32> &sids,
                            rrr::i32 *res,
                            const function<void()>& callback) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto exec = dynamic_cast<ClassicExecutor*>(GetExecutor(cmd_id));
  verify(exec != nullptr);
  string log;
  auto func = [exec, res, callback, this] () -> void {
    std::lock_guard<std::recursive_mutex> lock(mtx_);
    *res = exec->Prepare() ? SUCCESS : REJECT;
    callback();
  };
  if (Config::GetConfig()->IsReplicated()) {
    SimpleCommand cmd; // TODO
    CreateRepCoord()->Submit(cmd, func);
  } else if (Config::GetConfig()->do_logging()) {
    this->get_prepare_log(cmd_id, sids, &log);
    recorder_->submit(log, func);
  } else {
    func();
  }
  return 0;
}

int ClassicSched::OnCommit(cmdid_t cmd_id,
                           int commit_or_abort,
                           rrr::i32 *res,
                           const function<void()>& callback) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto exec = (ClassicExecutor*)GetExecutor(cmd_id);
  verify(exec->phase_ < 3);
  exec->phase_ = 3;
  if (commit_or_abort == SUCCESS) {
    exec->CommitLaunch(res, callback);
  } else if (commit_or_abort == REJECT) {
    exec->AbortLaunch(res, callback);
  } else {
    verify(0);
  }
  DestroyExecutor(cmd_id);
  return 0;
}
} // namespace rococo