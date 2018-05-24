#pragma once

#include "deptran/rococo/tx.h"
#include "../command.h"
#include "dep_graph.h"

namespace janus {

class TxJanus : public TxRococo {
public:
  using TxRococo::TxRococo;

  void DispatchExecute(SimpleCommand &cmd,
                       int *res,
                       map<int32_t, Value> *output) override;

};

} // namespace janus
