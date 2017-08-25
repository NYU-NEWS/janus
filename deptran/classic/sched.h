//
// Created by shuai on 11/25/15.
//
#pragma once

#include "../scheduler.h"

namespace janus {

class Procedure;
class TpcPrepareCommand;
class TpcCommitCommand;
class SimpleCommand;
class SchedulerClassic: public Scheduler {
 using Scheduler::Scheduler;
 public:
  virtual bool OnDispatch(TxnPieceData& piece_data,
                          TxnOutput& ret_output) override;

  virtual bool BeforeAccess(TxBox& tx_box, Row* row, int col_id) {
    Log_fatal("feature not implemented: before_access");
    return false;
  };

  virtual int OnDispatchOld(const vector<SimpleCommand> &cmds,
                            int32_t *res,
                            TxnOutput* output,
                            const function<void()>& callback);
  // PrepareRequest
  virtual bool OnPrepare(txnid_t tx_id,
                         const std::vector<i32> &sids);

  virtual bool DoPrepare(txnid_t tx_id) {
    Log_fatal("feature not implemented: do prepare");
  };

  virtual int OnCommit(cmdid_t cmd_id,
                       int commit_or_abort);

  virtual void DoCommit(TxBox& tx_box);

  virtual void DoAbort(TxBox& tx_box);

  void OnLearn(ContainerCommand&) override;

  int PrepareReplicated(TpcPrepareCommand& prepare_cmd);
  int CommitReplicated(TpcCommitCommand& commit_cmd);
};

} // namespace janus
