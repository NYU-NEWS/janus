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
class TxLogServer;
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
                                         shared_ptr<TxnRegistry> txn_reg);
  virtual Executor *CreateExecutor(cmdid_t cmd_id, TxLogServer *sch);
  virtual TxLogServer *CreateScheduler();
  virtual Communicator *CreateCommo(PollMgr *pollmgr);
  // for only dtxn
  Sharding *CreateSharding();
  Sharding *CreateSharding(Sharding *sd);
  virtual mdb::Row *CreateRow(const mdb::Schema *schema,
                              std::vector<Value> &row_data);
  void GetTxTypes(std::map<int32_t, std::string>& txn_types);
  TxData *CreateTxnCommand(TxRequest &req, shared_ptr<TxnRegistry> reg);
//  TxData *CreateChopper(TxRequest &req, TxnRegistry *reg);
  virtual shared_ptr<Tx> CreateTx(epoch_t epoch,
                                  txnid_t txn_id,
                                  bool ro,
                                  TxLogServer *sch);

  Workload *CreateTxGenerator();
  virtual vector<rrr::Service *> CreateRpcServices(uint32_t site_id,
                                                   TxLogServer *dtxn_sched,
                                                   rrr::PollMgr *poll_mgr,
                                                   ServerControlServiceImpl *scsi);
};

#define RANDOM_VAR_NAME(var, file, line) \
var##file##line

#define REG_FRAME(mode, names, classframe) \
static Frame* RANDOM_VAR_NAME(var, __FILE__, __LINE__) = \
Frame::RegFrame(mode, names, []()->Frame*{ return new classframe(mode);})

} // namespace janus
