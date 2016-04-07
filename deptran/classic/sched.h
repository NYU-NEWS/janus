//
// Created by shuai on 11/25/15.
//
#pragma once

#include "../none/sched.h"

namespace rococo {

class TxnCommand;
class TpcPrepareCommand;
class ClassicSched: public NoneSched {
 using NoneSched::NoneSched;
 public:

  virtual int OnDispatch(const SimpleCommand &cmd,
                         int32_t *res,
                         map<int32_t, Value> *output,
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

  int PrepareReplicated(TpcPrepareCommand& cmd);

};

} // namespace rococo
