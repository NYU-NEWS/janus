
#include "../constants.h"
#include "../dtxn.h"
#include "../txn_chopper.h"
#include "../coordinator.h"
#include "sched.h"
#include "../tpl/tpl.h"
#include "exec.h"
#include "tpc_command.h"

namespace rococo {

int ClassicSched::OnDispatch(const SimpleCommand &cmd,
                             int32_t *res,
                             map<int32_t, Value> *output,
                             const function<void()>& callback) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  verify(frame_);
  auto exec = (ClassicExecutor*) GetOrCreateExecutor(cmd.root_id_);
  exec->cmds_.push_back(cmd);
  exec->StartLaunch(cmd,
                    res,
                    output,
                    callback);
  return 0;
}


// On prepare with replication
//   1. dispatch the whole transaction to others.
//   2. use a paxos command to commit the prepare request.
//   3. after that, run the function to prepare.
//   0. an non-optimized version would be.
//      dispatch the transaction command with paxos instance
int ClassicSched::OnPrepare(cmdid_t cmd_id,
                            const std::vector<i32> &sids,
                            rrr::i32 *res,
                            const function<void()>& callback) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto exec = dynamic_cast<ClassicExecutor*>(GetExecutor(cmd_id));
  *res = exec->Prepare() ? SUCCESS : REJECT;

  if (Config::GetConfig()->IsReplicated()) {
//    SimpleCommand cmd; // TODO
    TpcPrepareCommand *cmd = new TpcPrepareCommand; // TODO watch out memory
    cmd->cmds_ = exec->cmds_;
    CreateRepCoord()->Submit(*cmd, callback);
  } else if (Config::GetConfig()->do_logging()) {
    string log;
    this->get_prepare_log(cmd_id, sids, &log);
    recorder_->submit(log, callback);
  } else {
    callback();
  }
  return 0;
}

int ClassicSched::PrepareReplicated(TpcPrepareCommand& cmd) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
//  auto exec = dynamic_cast<ClassicExecutor*>(GetExecutor(cmd_id));
  // TODO verify it is the same leader, error if not.
  // TODO and return the prepare callback here.
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
  TpcCommitCommand* cmd = new TpcCommitCommand;
  if (Config::GetConfig()->IsReplicated()) {
    CreateRepCoord()->Submit(*cmd);
  }
  return 0;
}

void ClassicSched::OnLearn(ContainerCommand& cmd) {
  if (cmd.type_ == CMD_TPC_PREPARE) {
    TpcPrepareCommand& c = (TpcPrepareCommand&)cmd;
    PrepareReplicated(c);
  } else if (cmd.type_ == CMD_TPC_COMMIT) {
    // TODO, execute for slave.
    // pass
  } else {
    verify(0);
  }
}

} // namespace rococo