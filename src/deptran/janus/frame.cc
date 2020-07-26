
#include "../__dep__.h"
#include "../command.h"
#include "../command_marshaler.h"
#include "../communicator.h"
#include "../rcc/row.h"
#include "commo.h"
#include "frame.h"
#include "coordinator.h"
#include "scheduler.h"
#include "tx.h"
#include "../troad/troad.h"

namespace janus {

REG_FRAME(MODE_JANUS, vector<string>({"brq","baroque","janus"}), TroadJanusFrame);

Coordinator *JanusFrame::CreateCoordinator(cooid_t coo_id,
                                           Config *config,
                                           int benchmark,
                                           ClientControlServiceImpl *ccsi,
                                           uint32_t id,
                                           shared_ptr<TxnRegistry> txn_reg) {
  verify(config != nullptr);
  auto *coord = new CoordinatorJanus(coo_id,
                                     benchmark,
                                     ccsi,
                                     id);
  coord->txn_reg_ = txn_reg;
  coord->frame_ = this;
  return coord;
}

Executor *JanusFrame::CreateExecutor(uint64_t, TxLogServer *sched) {
  verify(0);
  return nullptr;
}

TxLogServer *JanusFrame::CreateScheduler() {
  TxLogServer *sched = new SchedulerJanus();
  sched->frame_ = this;
  return sched;
}

vector<rrr::Service *>
JanusFrame::CreateRpcServices(uint32_t site_id,
                              TxLogServer *sched,
                              rrr::PollMgr *poll_mgr,
                              ServerControlServiceImpl *scsi) {
  return Frame::CreateRpcServices(site_id, sched, poll_mgr, scsi);
}

mdb::Row *JanusFrame::CreateRow(const mdb::Schema *schema,
                                vector<Value> &row_data) {

  mdb::Row *r = RccRow::create(schema, row_data);
  return r;
}

shared_ptr<Tx> JanusFrame::CreateTx(epoch_t epoch, txnid_t tid,
                                    bool ro, TxLogServer *mgr) {
//  auto dtxn = new JanusDTxn(tid, mgr, ro);
//  return dtxn;
  shared_ptr<Tx> sp_tx(new TxJanus(epoch, tid, mgr, ro));
  return sp_tx;
}

Communicator *JanusFrame::CreateCommo(PollMgr *poll) {
  return new JanusCommo(poll);
}

} // namespace janus
