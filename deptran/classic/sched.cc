
#include "../constants.h"
#include "../dtxn.h"
#include "../txn_chopper.h"
#include "../coordinator.h"
#include "sched.h"
#include "../tpl/tpl.h"
#include "exec.h"

namespace rococo {

int ClassicSched::OnDispatch(const SimpleCommand &cmd,
                                rrr::i32 *res,
                                map<int32_t, Value> *output,
                                rrr::DeferredReply *defer) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto exec = (ClassicExecutor*) GetOrCreateExecutor(cmd.root_id_);
  exec->StartLaunch(cmd,
                    res,
                    output,
                    defer);
  return 0;
}

int ClassicSched::OnPrepare(
    cmdid_t cmd_id,
    const std::vector<i32> &sids,
    rrr::i32 *res,
    rrr::DeferredReply *defer) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto exec = dynamic_cast<ClassicExecutor*>(GetExecutor(cmd_id));
  verify(exec != nullptr);
  string log;
  auto func = [exec, res, defer, this] () -> void {
    std::lock_guard<std::recursive_mutex> lock(mtx_);
    *res = exec->Prepare() ? SUCCESS : REJECT;
    defer->reply();
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
                              rrr::DeferredReply *defer) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto exec = (ClassicExecutor*)GetExecutor(cmd_id);
  verify(exec->phase_ < 3);
  exec->phase_ = 3;
  if (commit_or_abort == SUCCESS) {
    exec->commit_launch(res, defer);
  } else if (commit_or_abort == REJECT) {
    exec->abort_launch(res, defer);
  } else {
    verify(0);
  }
  DestroyExecutor(cmd_id);
  return 0;
}
} // namespace rococo