//
// Created by lamont on 1/11/16.
//
#pragma once

#include "deptran/classic/exec.h"
#include "deptran/scheduler.h"
#include "deptran/executor.h"
#include "MdccScheduler.h"

namespace mdcc {
  using rococo::Executor;
  using rococo::Scheduler;

  class MdccExecutor : public Executor {
  protected:
    MdccScheduler* sched_;
  public:
    MdccExecutor() = delete;
    MdccExecutor(txnid_t txn_id, Scheduler* sched);
    void StartPiece(const rococo::SimpleCommand &cmd, int *result, DeferredReply *defer);

    bool ValidRead(OptionSet &option);
  };
}
