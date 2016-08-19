//
// Created by shuai on 11/25/15.
//
#pragma once

#include "../scheduler.h"

namespace rococo {

class TxnCommand;
class TpcPrepareCommand;
class TpcCommitCommand;
class SimpleCommand;
class ClassicSched: public Scheduler {
 using Scheduler::Scheduler;
 public:

  virtual int OnDispatch(const vector<SimpleCommand> &cmds,
                         int32_t *res,
                         TxnOutput* output,
                         const function<void()>& callback);
  // PrepareRequest
  virtual int OnPrepare(cmdid_t cmd_id,
                        const std::vector<i32> &sids,
                        rrr::i32 *res,
                        const function<void()>& callback);
  virtual int OnCommit(cmdid_t cmd_id,
                       int commit_or_abort,
                       int32_t *res,
                       const function<void()>& callback);

  void OnLearn(ContainerCommand&) override;

  int PrepareReplicated(TpcPrepareCommand& prepare_cmd);
  int CommitReplicated(TpcCommitCommand& commit_cmd);
};

} // namespace rococo
