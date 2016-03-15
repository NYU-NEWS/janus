#pragma once

#include "../classic/exec.h"

namespace rococo {

class OCCExecutor: public ThreePhaseExecutor {
 public:
  using ThreePhaseExecutor::ThreePhaseExecutor;
  virtual int StartLaunch(const SimpleCommand& cmd,
                          rrr::i32 *res,
                          map<int32_t, Value> &output,
                          rrr::DeferredReply *defer);

  virtual bool Prepare() override;
  virtual int Commit() override;

};

} // namespace rococo