#pragma once

#include "../rcc/dtxn.h"
#include "../command.h"
#include "dep_graph.h"
#include "brq-common.h"

namespace rococo {

class BrqDTxn : public RccDTxn {
public:
  using RccDTxn::RccDTxn;

  void DispatchExecute(SimpleCommand &cmd,
                       int *res,
                       map<int32_t, Value> *output) override;

};

} // namespace rococo
