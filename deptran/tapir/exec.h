#pragma once

#include "../executor.h"

namespace rococo {
class TapirDTxn;
class TapirExecutor : public Executor {
 public:
  using Executor::Executor;

  void FastAccept(int *res);

  TapirDTxn* dtxn();
};

} // namespace rococo