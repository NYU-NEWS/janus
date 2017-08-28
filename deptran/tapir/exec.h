#pragma once

#include "../__dep__.h"
#include "../executor.h"

namespace janus {
class TxTapir;
class TapirExecutor : public Executor {
 public:
  using Executor::Executor;
  set<VersionedRow*> locked_rows_{};
  map<VersionedRow*, map<colid_t, uint64_t>> prepared_rvers_{};
  map<VersionedRow*, map<colid_t, uint64_t>> prepared_wvers_{};

  static set<Row*> locked_rows_s; // only for debug.

  int FastAccept(const vector<SimpleCommand>& txn_cmds);
  void Commit();
  void Abort();
  void Cleanup();

  shared_ptr<TxTapir> dtxn();
};

} // namespace janus