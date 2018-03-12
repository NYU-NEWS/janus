//
// Created by shuai on 11/25/15.
//
#pragma once

#include "../scheduler.h"

namespace janus {

class TxData;
class TpcPrepareCommand;
class TpcCommitCommand;
class SimpleCommand;
class SchedulerClassic: public Scheduler {
 using Scheduler::Scheduler;
 public:
  virtual bool OnDispatch(vector<TxPieceData>& piece_data,
                          TxnOutput& ret_output) override;

  virtual bool Guard(Tx &tx_box, Row *row, int col_id, bool write=true) {
    Log_fatal("feature not implemented: before_access");
    return false;
  };

  // PrepareRequest
  virtual bool OnPrepare(txnid_t tx_id,
                         const std::vector<i32> &sids);

  virtual bool DoPrepare(txnid_t tx_id) {
    Log_fatal("feature not implemented: do prepare");
    return false;
  };

  virtual int OnCommit(cmdid_t cmd_id,
                       int commit_or_abort);

  virtual void DoCommit(Tx& tx_box);

  virtual void DoAbort(Tx& tx_box);

  void OnLearn(CmdData&) override;

  int PrepareReplicated(TpcPrepareCommand& prepare_cmd);
  int CommitReplicated(TpcCommitCommand& commit_cmd);
};

} // namespace janus
