
#include "../constants.h"
#include "../dtxn.h"
#include "../txn_chopper.h"
#include "../coordinator.h"
#include "sched.h"
#include "../tpl/tpl.h"
#include "exec.h"
#include "tpc_command.h"

namespace rococo {

int ClassicSched::OnDispatch(const vector<SimpleCommand>& cmd,
                             int32_t *res,
                             TxnOutput* output,
                             const function<void()>& callback) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  verify(frame_);
  auto exec = (ClassicExecutor*) GetOrCreateExecutor(cmd[0].root_id_);
  vector<int32_t> *rr = new vector<int32_t>(cmd.size());

  // XXX just touch in case
  for (auto c: cmd) {
    (*output)[c.inn_id()];
  }

  // TODO
  verify(cmd.size() > 0);
  int* sz = new int(cmd.size());
  int* i = new int(0);
  function<void()>* func = new function<void()>();
  TxnWorkspace* ws = new TxnWorkspace();
  *res = SUCCESS;
  *func = [sz, i, func, &cmd, rr, exec, output, ws, res, callback, this] () {
    std::lock_guard<std::recursive_mutex> lock(mtx_);
    verify(*sz == cmd.size());

    if (*res == REJECT) {
      callback();
      return;
    }

    if (*i > 0) { // not first time called.
      int j = (*i) - 1;
      verify(j >= 0);
      verify(j < cmd.size());
      auto& m = (*output)[cmd[j].inn_id()];
      if (cmd[j].inn_id() == 1000) {
        verify(m.count(1011) > 0);
      }
      ws->insert(m);
    }
    if (*i == *sz) {
      callback();
      return;
    } else if (*i < *sz) {
      int j = *i;
      (*i)++;
      const_cast<SimpleCommand&>(cmd[j]).input.Aggregate(*ws);
      exec->cmds_.push_back(cmd[j]);
      exec->OnDispatch(cmd[j],
                       res,
                       &(*output)[cmd[j].inn_id()],
                       *func);
    }

  };

  (*func)();

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
  Log_debug("%s: at site %d", __FUNCTION__, this->site_id_);
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto exec = dynamic_cast<ClassicExecutor*>(GetExecutor(cmd_id));
  exec->prepare_reply_ = [res, callback] (int r) {*res = r; callback();};

  if (Config::GetConfig()->IsReplicated()) {
    TpcPrepareCommand *cmd = new TpcPrepareCommand; // TODO watch out memory
    cmd->txn_id_ = cmd_id;
    cmd->cmds_ = exec->cmds_;
    CreateRepCoord()->Submit(*cmd);
  } else if (Config::GetConfig()->do_logging()) {
    string log;
    this->get_prepare_log(cmd_id, sids, &log);
    recorder_->submit(log, callback);
  } else {
    auto exec = dynamic_cast<ClassicExecutor*>(GetExecutor(cmd_id));
    *res = exec->Prepare() ? SUCCESS : REJECT;
    callback();
  }
  return 0;
}

int ClassicSched::PrepareReplicated(TpcPrepareCommand& prepare_cmd) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  // TODO verify it is the same leader, error if not.
  // TODO and return the prepare callback here.
  auto tid = prepare_cmd.txn_id_;
  auto exec = dynamic_cast<ClassicExecutor*>(GetOrCreateExecutor(tid));
  verify(prepare_cmd.cmds_.size() > 0);
  if (exec->cmds_.size() == 0)
    exec->cmds_ = prepare_cmd.cmds_;
  // else: is the leader.
  verify(exec->cmds_.size() > 0);
  int r = exec->Prepare() ? SUCCESS : REJECT;
  exec->prepare_reply_(r);
  return 0;
}

int ClassicSched::OnCommit(cmdid_t cmd_id,
                           int commit_or_abort,
                           rrr::i32 *res,
                           const function<void()>& callback) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto exec = (ClassicExecutor*)GetExecutor(cmd_id);
  if (Config::GetConfig()->IsReplicated()) {
    exec->commit_reply_ = [callback] (int r) {callback();};
    TpcCommitCommand* cmd = new TpcCommitCommand;
    cmd->txn_id_ = cmd_id;
    cmd->res_ = commit_or_abort;
    CreateRepCoord()->Submit(*cmd);
  } else {
    verify(exec->phase_ < 3);
    exec->phase_ = 3;
    if (commit_or_abort == SUCCESS) {
      exec->CommitLaunch(res, callback);
    } else if (commit_or_abort == REJECT) {
      exec->AbortLaunch(res, callback);
    } else {
      verify(0);
    }
    TrashExecutor(cmd_id);
  }
  return 0;
}

int ClassicSched::CommitReplicated(TpcCommitCommand& tpc_commit_cmd) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto txn_id = tpc_commit_cmd.txn_id_;
  auto exec = (ClassicExecutor*) GetOrCreateExecutor(txn_id);
  verify(exec->phase_ < 3);
  exec->phase_ = 3;
  int commit_or_abort = tpc_commit_cmd.res_;
  if (commit_or_abort == SUCCESS) {
    exec->Commit();
  } else if (commit_or_abort == REJECT) {
    exec->Abort();
  } else {
    verify(0);
  }
  exec->commit_reply_(SUCCESS);
  TrashExecutor(txn_id);
}

void ClassicSched::OnLearn(ContainerCommand& cmd) {
  if (cmd.type_ == CMD_TPC_PREPARE) {
    TpcPrepareCommand& c = dynamic_cast<TpcPrepareCommand&>(*cmd.self_cmd_);
    PrepareReplicated(c);
  } else if (cmd.type_ == CMD_TPC_COMMIT) {
    TpcCommitCommand& c = dynamic_cast<TpcCommitCommand&>(*cmd.self_cmd_);
    CommitReplicated(c);
  } else {
    verify(0);
  }
}

} // namespace rococo