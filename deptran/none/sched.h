#pragma once

#include "../classic/sched.h"

namespace rococo {

class NoneSched: public ClassicSched {
 using ClassicSched::ClassicSched;
 public:
  virtual int OnDispatch(const vector<SimpleCommand> &cmd,
                         rrr::i32 *res,
                         TxnOutput* output,
                         const function<void()>& callback) override;
};

}