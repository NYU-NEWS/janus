#pragma once
//#include "__dep__.h"
#include "constants.h"
#include "txn_req_factory.h"
#include "txn_reg.h"

namespace rococo {

class Sharding;
class Coordinator;
class TxnChopper;
class TxnRequest;
class DTxn;
class Executor;
class Scheduler;
class ClientControlServiceImpl;
class TxnRegistry;
class Frame {
 public:
  Sharding* CreateSharding();
  Sharding* CreateSharding(Sharding* sd);
  mdb::Row* CreateRow(const mdb::Schema *schema, std::vector<Value> &row_data);
  Coordinator* CreateCoord(cooid_t coo_id, vector<std::string>& servers,
                           int benchmark, int mode,
                           ClientControlServiceImpl *ccsi,
                           uint32_t id, bool batch_start);
  void GetTxnTypes(std::map<int32_t, std::string> &txn_types);
  TxnChopper* CreateChopper(TxnRequest &req, TxnRegistry *reg);
  DTxn* CreateDTxn(txnid_t txn_id, bool ro, Scheduler *sch);
  Executor* CreateExecutor(cmdid_t cmd_id, Scheduler *sch);
  Scheduler *CreateScheduler();
  TxnGenerator * CreateTxnGenerator();
};


} // namespace rococo