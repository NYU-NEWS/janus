#pragma once

#include "deptran/janus/tx.h"
#include "../command.h"

namespace janus {

class TxTroad : public TxJanus {
public:
  using TxJanus::TxJanus;

  void DispatchExecute(SimpleCommand &cmd,
                       map<int32_t, Value> *output) override;

};

} // namespace janus
