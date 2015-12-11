#pragma once

#include "__dep__.h"
#include "constants.h"

namespace rococo {

class TxnRegistry;
class Scheduler;
class DTxn;
class Executor {
 public:
  Recorder* recorder_ = nullptr;
  TxnRegistry* txn_reg_ = nullptr;
  mdb::Txn *mdb_txn_ = nullptr;
  Scheduler* sched_ = nullptr;
  DTxn* dtxn_ = nullptr;
  cmdid_t cmd_id_ = 0;
  int phase_ = -1;

  Executor(cmdid_t cmd_id, Scheduler* sched);
  virtual ~Executor();
};

} // namespace rococo