#pragma once
#include "__dep__.h"
#include "constants.h"
#include "txn_reg.h"
#include "config.h"

namespace janus {

class Sharding;
class Coordinator;
class CoordinatorBase;
class TxData;
class TxRequest;
class Tx;
class Executor;
class Scheduler;
class ClientControlServiceImpl;
class ServerControlServiceImpl;
class TxnRegistry;
class Communicator;
class Workload;
class Frame {
 public:
  Communicator *commo_ = nullptr;

  // static variables to hold frames
  static map<string, int> &FrameNameToMode();
  static map<int, function<Frame *()>> &ModeToFrame();
  static int Name2Mode(string name);
  static Frame *GetFrame(int mode);
  static Frame *GetFrame(string name);
  static Frame *RegFrame(int mode, function<Frame *()>); // deprecated.
  static Frame *RegFrame(int mode, vector<string> names, function<Frame *()>);

  int mode_;
  Config::SiteInfo *site_info_ = nullptr;
  Frame(int mode) : mode_(mode) {};
  // for both dtxn and rep
  virtual Coordinator *CreateCoordinator(cooid_t coo_id,
                                         Config *config,
                                         int benchmark,
                                         ClientControlServiceImpl *ccsi,
                                         uint32_t id,
                                         TxnRegistry *txn_reg);
  virtual Executor *CreateExecutor(cmdid_t cmd_id, Scheduler *sch);
  virtual Scheduler *CreateScheduler();
  virtual Communicator *CreateCommo(PollMgr *pollmgr = nullptr);
  // for only dtxn
  Sharding *CreateSharding();
  Sharding *CreateSharding(Sharding *sd);
  virtual mdb::Row *CreateRow(const mdb::Schema *schema,
                              std::vector<Value> &row_data);
  void GetTxTypes(std::map<int32_t, std::string>& txn_types);
  TxData *CreateTxnCommand(TxRequest &req, TxnRegistry *reg);
  TxData *CreateChopper(TxRequest &req, TxnRegistry *reg);
  virtual shared_ptr<Tx> CreateTx(epoch_t epoch,
                                  txnid_t txn_id,
                                  bool ro,
                                  Scheduler *sch);

  Workload *CreateTxGenerator();
  virtual vector<rrr::Service *> CreateRpcServices(uint32_t site_id,
                                                   Scheduler *dtxn_sched,
                                                   rrr::PollMgr *poll_mgr,
                                                   ServerControlServiceImpl *scsi);
};

} // namespace janus
