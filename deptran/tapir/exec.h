#pragma once

#include "../__dep__.h"
#include "../executor.h"

namespace rococo {
class TapirDTxn;
class TapirExecutor : public Executor {
 public:
  using Executor::Executor;
  set<VersionedRow*> locked_rows_{};
  map<VersionedRow*, map<column_id_t, uint64_t>> prepared_rvers_{};
  map<VersionedRow*, map<column_id_t, uint64_t>> prepared_wvers_{};

  static set<Row*> locked_rows_s; // only for debug.

  void FastAccept(const vector<SimpleCommand>& txn_cmds,
                  int32_t *res);
  void Commit();
  void Abort();
  void Cleanup();

  TapirDTxn* dtxn();
};

} // namespace rococo