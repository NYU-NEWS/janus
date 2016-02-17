#include "../constants.h"
#include "frame.h"
#include "exec.h"
#include "coord.h"
#include "sched.h"
#include "commo.h"
#include "config.h"

namespace rococo {

static Frame* tapir_frame_s = Frame::RegFrame(MODE_TAPIR,
                                              {"tapir"},
                                              new TapirFrame());

Executor* TapirFrame::CreateExecutor(cmdid_t cmd_id, Scheduler* sched) {
  Executor* exec = new TapirExecutor(cmd_id, sched);
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
  coord->frame_ = this;
  return coord;
}

Scheduler* TapirFrame::CreateScheduler() {
  Scheduler* sched = new TapirSched();

  return sched;
}

vector<rrr::Service *>
TapirFrame::CreateRpcServices(uint32_t site_id,
                              Scheduler *dtxn_sched,
                              rrr::PollMgr *poll_mgr,
                              ServerControlServiceImpl* scsi) {
  return {};
}

mdb::Row* TapirFrame::CreateRow(const mdb::Schema *schema,
                                vector<Value>& row_data) {

  verify(0);
  return nullptr;
}

} // namespace rococo
