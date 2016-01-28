#pragma once
//#include "__dep__.h"
#include "constants.h"
#include "txn_req_factory.h"
#include "txn_reg.h"

namespace rococo {

class Sharding;
class Coordinator;
class CoordinatorBase;
class TxnCommand;
class TxnRequest;
class DTxn;
class Executor;
class Scheduler;
class ClientControlServiceImpl;
class ServerControlServiceImpl;
class TxnRegistry;
class Config;
class Frame {
 public:
  Sharding* CreateSharding();
  Sharding* CreateSharding(Sharding* sd);
  mdb::Row* CreateRow(const mdb::Schema *schema, std::vector<Value> &row_data);
  CoordinatorBase* CreateCoord(cooid_t coo_id,
                               vector<std::string>& servers,
                               Config* config,
                               int benchmark, int mode,
                               ClientControlServiceImpl *ccsi,
                               uint32_t id,
                               bool batch_start,
                               TxnRegistry* txn_reg);
  Coordinator* CreateRepCoord(cooid_t coo_id,
                              vector<std::string>& servers,
                              Config* config,
                              int benchmark, int mode,
                              ClientControlServiceImpl *ccsi,
                              uint32_t id,
                              bool batch_start,
                              TxnRegistry* txn_reg);
  void GetTxnTypes(std::map<int32_t, std::string> &txn_types);
  TxnCommand* CreateTxnCommand(TxnRequest& req, TxnRegistry* reg);
  TxnCommand * CreateChopper(TxnRequest &req, TxnRegistry *reg);
  DTxn* CreateDTxn(txnid_t txn_id, bool ro, Scheduler *sch);
  Executor* CreateExecutor(cmdid_t cmd_id, Scheduler *sch);
  Scheduler *CreateScheduler();
  TxnGenerator * CreateTxnGenerator();

  vector<rrr::Service*> CreateRpcServices(
      Config* config,
      uint32_t site_id,
      Scheduler *dtxn_mgr,
      rrr::PollMgr* poll_mgr,
      ServerControlServiceImpl *scsi);
};


} // namespace rococo