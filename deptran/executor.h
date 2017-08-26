#pragma once

#include "__dep__.h"
#include "constants.h"

namespace rococo {

class TxnRegistry;
class Scheduler;
class TxBox;
class SimpleCommand;
class Procedure;
class Executor {
 public:
  Recorder* recorder_ = nullptr;
  TxnRegistry* txn_reg_ = nullptr;
  mdb::Txn *mdb_txn_ = nullptr;
  Scheduler* sched_ = nullptr;
  TxBox* dtxn_ = nullptr;
  Procedure* txn_cmd_ = nullptr;
  cmdid_t cmd_id_ = 0;
  int phase_ = -1;

  Executor() = delete;
  Executor(txnid_t txn_id, Scheduler* sched);
  virtual void Execute(const SimpleCommand &cmd,
                       rrr::i32 *res,
                       map<int32_t, Value> &output);
  virtual void Execute(const vector<SimpleCommand>& cmd,
                       TxnOutput* output) ;
  virtual ~Executor();
  mdb::Txn* mdb_txn();
};

} // namespace rococo