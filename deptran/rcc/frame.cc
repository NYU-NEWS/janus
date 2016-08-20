#include "../constants.h"
#include "frame.h"
//#include "exec.h"
//#include "coord.h"
#include "coord.h"
#include "sched.h"
#include "dtxn.h"
#include "commo.h"
#include "config.h"

namespace rococo {

static Frame* rcc_frame_s = Frame::RegFrame(MODE_RCC,
                                            {"rcc", "rococo"},
                                            [] () -> Frame* {
                                              return new RccFrame();
                                            });

Executor* RccFrame::CreateExecutor(cmdid_t cmd_id, Scheduler* sched) {
  verify(0);
  Executor* exec = nullptr;
//  Executor* exec = new TapirExecutor(cmd_id, sched);
  return exec;
}

Coordinator* RccFrame::CreateCoord(cooid_t coo_id,
                                   Config* config,
                                   int benchmark,
                                   ClientControlServiceImpl *ccsi,
                                   uint32_t id,
                                   TxnRegistry* txn_reg) {
  verify(config != nullptr);
  RccCoord* coord = new RccCoord(coo_id,
                                 benchmark,
                                 ccsi,
                                 id);
  coord->txn_reg_ = txn_reg;
  coord->frame_ = this;
  return coord;
}

Scheduler* RccFrame::CreateScheduler() {
  Scheduler* sched = new RccSched();
  sched->frame_ = this;
  return sched;
}

vector<rrr::Service *>
RccFrame::CreateRpcServices(uint32_t site_id,
                            Scheduler *sched,
                            rrr::PollMgr *poll_mgr,
                            ServerControlServiceImpl* scsi) {
//  auto config = Config::GetConfig();
//  auto result = std::vector<Service *>();
//  auto s = new RococoServiceImpl(sched, poll_mgr, scsi);
//  result.push_back(s);
//  return result;
  return Frame::CreateRpcServices(site_id, sched, poll_mgr, scsi);
}

mdb::Row* RccFrame::CreateRow(const mdb::Schema *schema,
                              vector<Value>& row_data) {
  mdb::Row* r = RCCRow::create(schema, row_data);
  return r;
}

DTxn* RccFrame::CreateDTxn(epoch_t epoch, txnid_t tid,
                           bool ro, Scheduler *mgr) {
  auto dtxn = new RccDTxn(epoch, tid, mgr, ro);
  return dtxn;
}

Communicator* RccFrame::CreateCommo(PollMgr* poll) {
  return new RccCommo(poll);
}

} // namespace rococo
