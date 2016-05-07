#pragma once

#include "../classic/exec.h"

namespace rococo {

class OCCExecutor: public ClassicExecutor {
 public:
  using ClassicExecutor::ClassicExecutor;
  int OnDispatch(const SimpleCommand &cmd,
                 rrr::i32 *res,
                 map<int32_t, Value> *output,
                 const function<void()> &callback) override;

  virtual bool Prepare() override;
  virtual int Commit() override;

};

} // namespace rococo