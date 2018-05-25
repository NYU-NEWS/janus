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

  void MergeCommands(shared_ptr<Marshallable> cmd1,
                     shared_ptr<Marshallable> cmd2);

  bool ExecutePiece(Tx& tx, TxPieceData& piece_data, TxnOutput& ret_output);

  virtual bool DispatchPiece(Tx& tx,
                             TxPieceData& cmd,
                             TxnOutput& ret_output);

  virtual bool Dispatch(cmdid_t cmd_id,
                        shared_ptr<Marshallable> cmd,
                        TxnOutput& ret_output) override;

  /**
   * For interactive pre-processing.
   * @param tx_box
   * @param row
   * @param col_id
   * @param write
   * @return
   */
  virtual bool Guard(Tx &tx_box, Row *row, int col_id, bool write=true) = 0;

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

  bool ExecuteAll(Tx &tx, TxnOutput &ret_output);

  void Next(Marshallable&) override;

  int PrepareReplicated(TpcPrepareCommand& prepare_cmd);
  int CommitReplicated(TpcCommitCommand& commit_cmd);

};

} // namespace janus
