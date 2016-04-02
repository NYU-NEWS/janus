
#include "../__dep__.h"
#include "../command.h"
#include "../command_marshaler.h"
#include "../communicator.h"
#include "commo.h"
#include "frame.h"
#include "coord.h"
#include "sched.h"
#include "dep_graph.h"
#include "service.h"
#include "../rcc/rcc_row.h"
#include "../rcc/dtxn.h"

namespace rococo {

static Frame* brq_frame_s = Frame::RegFrame(MODE_BRQ,
                                            {"brq", "baroque", "janus"},
                                            new BrqFrame());


Coordinator* BrqFrame::CreateCoord(cooid_t coo_id,
                                   Config* config,
                                   int benchmark,
                                   ClientControlServiceImpl *ccsi,
                                   uint32_t id,
                                   TxnRegistry* txn_reg) {
  verify(config != nullptr);
  BrqCoord* coord = new BrqCoord(coo_id,
                                 benchmark,
                                 ccsi,
                                 id);
  coord->txn_reg_ = txn_reg;
  coord->frame_ = this;
  return coord;
}

Executor* BrqFrame::CreateExecutor(uint64_t, Scheduler *sched) {
  verify(0);
  return nullptr;
}


Scheduler* BrqFrame::CreateScheduler() {
  Scheduler* sched = new BrqSched();
  sched->frame_ = this;
  return sched;
}

vector<rrr::Service *>
BrqFrame::CreateRpcServices(uint32_t site_id,
                            Scheduler *sched,
                            rrr::PollMgr *poll_mgr,
                            ServerControlServiceImpl* scsi) {
  auto config = Config::GetConfig();
  auto result = vector<Service *>();
  auto s = new BrqServiceImpl(sched, poll_mgr, scsi);
  result.push_back(s);
  return result;
}

mdb::Row* BrqFrame::CreateRow(const mdb::Schema *schema,
                              vector<Value>& row_data) {

  mdb::Row* r = RCCRow::create(schema, row_data);
  return r;
}

DTxn* BrqFrame::CreateDTxn(txnid_t tid, bool ro, Scheduler * mgr) {
//  auto dtxn = new BrqDTxn(tid, mgr, ro);
//  return dtxn;
  auto dtxn = new RccDTxn(tid, mgr, ro);
  return dtxn;
}

Communicator* BrqFrame::CreateCommo(PollMgr* poll) {
  return new BrqCommo(poll);
}

}