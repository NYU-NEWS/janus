
#include "../constants.h"
#include "../dtxn.h"
#include "../txn_chopper.h"
#include "../coordinator.h"
#include "sched.h"
#include "../tpl/tpl.h"
#include "exec.h"

namespace rococo {

int ThreePhaseSched::OnPhaseOneRequest(const SimpleCommand &cmd,
                                       rrr::i32 *res,
                                       map<int32_t, Value> *output,
                                       rrr::DeferredReply *defer) {
  auto exec = (ThreePhaseExecutor*) GetOrCreateExecutor(cmd.root_id_);
  exec->StartLaunch(cmd,
                    res,
                    *output,
                    defer);
  defer->reply();
  return 0;
}

int ThreePhaseSched::OnPhaseTwoRequest(
    cmdid_t cmd_id,
    const std::vector <i32> &sids,
    rrr::i32 *res,
    rrr::DeferredReply *defer) {
  auto exec = (ThreePhaseExecutor*)GetExecutor(cmd_id);
  string log;
  auto func = [exec, res, defer] () -> void {
    *res = exec->Prepare();
    defer->reply();
  };
  if (Config::GetConfig()->IsReplicated()) {
    SimpleCommand cmd; // TODO
    GetRepCoord()->Submit(cmd, func);
  } else if (Config::GetConfig()->do_logging()) {
    this->get_prepare_log(cmd_id, sids, &log);
    recorder_->submit(log, func);
  } else {
    func();
  }
  return 0;
}

int ThreePhaseSched::OnPhaseThreeRequest(cmdid_t cmd_id,
                                         int commit_or_abort,
                                         rrr::i32 *res,
                                         rrr::DeferredReply *defer) {
  auto exec = (ThreePhaseExecutor*)GetExecutor(cmd_id);
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