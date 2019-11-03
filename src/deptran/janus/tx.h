#pragma once

#include "../rcc/tx.h"
#include "../command.h"

namespace janus {

class TxJanus : public RccTx {
public:
  using RccTx::RccTx;

  void DispatchExecute(SimpleCommand &cmd,
                       map<int32_t, Value> *output) override;

};

} // namespace janus
