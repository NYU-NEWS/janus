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
class Communicator;
class Frame {
 public:
  // static variables to hold frames
  static map<int, Frame*> frame_s_;
  static Frame* GetFrame(int mode);
  static Frame* RegFrame(int mode, Frame* frame);

  int mode_;
  Frame(int mode) : mode_(mode) {};
  // for both dtxn and rep
  virtual Coordinator* CreateCoord(cooid_t coo_id,
                           Config* config,
                           int benchmark,
                           ClientControlServiceImpl *ccsi,
                           uint32_t id,
                           bool batch_start,
                           TxnRegistry* txn_reg);
  virtual Executor* CreateExecutor(cmdid_t cmd_id, Scheduler *sch);
  virtual Scheduler *CreateScheduler();
  virtual Communicator* CreateCommo();
  // for only dtxn
  Sharding* CreateSharding();
  Sharding* CreateSharding(Sharding* sd);
  mdb::Row* CreateRow(const mdb::Schema *schema, std::vector<Value> &row_data);
  void GetTxnTypes(std::map<int32_t, std::string> &txn_types);
  TxnCommand* CreateTxnCommand(TxnRequest& req, TxnRegistry* reg);
  TxnCommand * CreateChopper(TxnRequest &req, TxnRegistry *reg);
  DTxn* CreateDTxn(txnid_t txn_id, bool ro, Scheduler *sch);

  TxnGenerator * CreateTxnGenerator();
  virtual vector<rrr::Service*> CreateRpcServices(uint32_t site_id,
                                                  Scheduler *dtxn_sched,
                                                  rrr::PollMgr* poll_mgr,
                                                  ServerControlServiceImpl *scsi);


//  Coordinator* CreateRepCoord(cooid_t coo_id,
//                              Config* config,
//                              int benchmark,
//                              ClientControlServiceImpl *ccsi,
//                              uint32_t id,
//                              bool batch_start,
//                              TxnRegistry* txn_reg);
//  Scheduler* CreateRepSched();
//  vector<rrr::Service*> CreateRepRpcServices(uint32_t site_id,
//                                             Scheduler *dtxn_sched,
//                                             rrr::PollMgr* poll_mgr,
//                                             ServerControlServiceImpl *scsi);
};


} // namespace rococo