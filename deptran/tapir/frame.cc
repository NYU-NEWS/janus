#include "../constants.h"
#include "frame.h"
#include "exec.h"
#include "coord.h"
#include "sched.h"
#include "commo.h"
#include "config.h"
#include "service.h"

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
  coord->txn_reg_ = txn_reg;
  coord->frame_ = this;
  return coord;
}

Scheduler* TapirFrame::CreateScheduler() {
  Scheduler* sched = new TapirSched();

  return sched;
}

vector<rrr::Service *>
TapirFrame::CreateRpcServices(uint32_t site_id,
                              Scheduler *sched,
                              rrr::PollMgr *poll_mgr,
                              ServerControlServiceImpl* scsi) {
  auto config = Config::GetConfig();
  auto result = std::vector<Service *>();
  verify(config->ab_mode_ == config->cc_mode_ &&
      config->ab_mode_ == MODE_TAPIR);
  result.push_back(new TapirServiceImpl(sched));
  return result;
}

mdb::Row* TapirFrame::CreateRow(const mdb::Schema *schema,
                                vector<Value>& row_data) {

  mdb::Row* r = mdb::VersionedRow::create(schema, row_data);
  return r;
}

} // namespace rococo
