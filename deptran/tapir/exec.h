#pragma once

#include "../__dep__.h"
#include "../executor.h"

namespace rococo {
class TapirDTxn;
class TapirExecutor : public Executor {
 public:
  using Executor::Executor;
  set<VersionedRow*> locked_rows_ = {};

  void FastAccept(const vector<SimpleCommand>& txn_cmds,
                  int32_t *res);
  void Commit();
  void Abort();

  TapirDTxn* dtxn();
};

} // namespace rococo