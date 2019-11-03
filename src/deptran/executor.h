#pragma once

#include "__dep__.h"
#include "constants.h"

namespace janus {

class TxnRegistry;
class TxLogServer;
class Tx;
class SimpleCommand;
class TxData;
class Executor {
 public:
  Recorder* recorder_ = nullptr;
  TxnRegistry* txn_reg_ = nullptr;
  mdb::Txn *mdb_txn_ = nullptr;
  TxLogServer* sched_ = nullptr;
  shared_ptr<Tx> dtxn_ = nullptr;
  TxData* txn_cmd_ = nullptr;
  cmdid_t cmd_id_ = 0;
  int phase_ = -1;

  Executor() = delete;
  Executor(txnid_t txn_id, TxLogServer* sched);
  virtual void Execute(const SimpleCommand &cmd,
                       rrr::i32 *res,
                       map<int32_t, Value> &output);
  virtual void Execute(const vector<SimpleCommand>& cmd,
                       TxnOutput* output) ;
  virtual ~Executor();
  mdb::Txn* mdb_txn();
};

} // namespace janus
