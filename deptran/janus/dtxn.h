#pragma once

#include "../rococo/dtxn.h"
#include "../command.h"
#include "dep_graph.h"
#include "brq-common.h"

namespace rococo {

class JanusDTxn : public RccDTxn {
public:
  using RccDTxn::RccDTxn;

  void DispatchExecute(SimpleCommand &cmd,
                       int *res,
                       map<int32_t, Value> *output) override;

};

} // namespace rococo
