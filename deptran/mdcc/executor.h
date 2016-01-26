//
// Created by lamont on 1/11/16.
//
#pragma once

#include "deptran/three_phase/exec.h"
#include "deptran/scheduler.h"
#include "deptran/executor.h"

namespace mdcc {
  using rococo::Executor;
  using rococo::Scheduler;

  class MdccExecutor : public Executor {
  public:
    MdccExecutor() = delete;
    MdccExecutor(txnid_t txn_id, Scheduler* sched);

    i8 Start(uint32_t txn_type, const map<int32_t, Value> &inputs);
    void StartPiece(const rococo::SimpleCommand& cmd, int* result);
  };
}
