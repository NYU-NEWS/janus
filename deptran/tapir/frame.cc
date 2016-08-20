#include "../constants.h"
#include "frame.h"
#include "exec.h"
#include "coord.h"
#include "sched.h"
#include "dtxn.h"
#include "commo.h"
#include "config.h"

namespace rococo {

static Frame* tapir_frame_s = Frame::RegFrame(MODE_TAPIR,
                                              {"tapir"},
                                              [] () -> Frame* {
                                                return new TapirFrame();
                                              });

Executor* TapirFrame::CreateExecutor(cmdid_t cmd_id, Scheduler* sched) {
  Executor* exec = new TapirExecutor(cmd_id, sched);
  return exec;
}

Coordinator* TapirFrame::CreateCoord(cooid_t coo_id,
                                     Config* config,
                                     int benchmark,
                                     ClientControlServiceImpl *ccsi,
                                     uint32_t id,
                                     TxnRegistry* txn_reg) {
  verify(config != nullptr);
  TapirCoord* coord = new TapirCoord(coo_id,
                                     benchmark,
                                     ccsi,
                                     id);
  coord->txn_reg_ = txn_reg;
  coord->frame_ = this;
  return coord;
}

Communicator* TapirFrame::CreateCommo(PollMgr* pollmgr) {
  // Default: return null;
  commo_ = new TapirCommo;
  return commo_;
}


Scheduler* TapirFrame::CreateScheduler() {
  Scheduler* sched = new TapirSched();
  sched->frame_ = this;
  return sched;
}

vector<rrr::Service *>
TapirFrame::CreateRpcServices(uint32_t site_id,
                              Scheduler *sched,
                              rrr::PollMgr *poll_mgr,
                              ServerControlServiceImpl* scsi) {
  auto config = Config::GetConfig();
  verify(config->ab_mode_ == config->cc_mode_ &&
      config->ab_mode_ == MODE_TAPIR);
  return Frame::CreateRpcServices(site_id, sched, poll_mgr, scsi);
}

mdb::Row* TapirFrame::CreateRow(const mdb::Schema *schema,
                                vector<Value>& row_data) {

  mdb::Row* r = mdb::VersionedRow::create(schema, row_data);
  return r;
}

DTxn* TapirFrame::CreateDTxn(epoch_t epoch, txnid_t tid,
                             bool ro, Scheduler * mgr) {
  auto dtxn = new TapirDTxn(epoch, tid, mgr);
  return dtxn;
}

} // namespace rococo
