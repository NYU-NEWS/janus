#pragma once

#include "deptran/rococo/tx.h"
#include "../command.h"
#include "dep_graph.h"
#include "brq-common.h"

namespace rococo {

class JanusDTxn : public TxRococo {
public:
  using TxRococo::TxRococo;

  void DispatchExecute(SimpleCommand &cmd,
                       int *res,
                       map<int32_t, Value> *output) override;

};

} // namespace rococo
