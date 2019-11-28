
#include "../__dep__.h"
#include "../command.h"
#include "../command_marshaler.h"
#include "../communicator.h"
#include "tx.h"
#include "frame.h"
#include "scheduler.h"
#include "coordinator.h"

namespace janus {

REG_FRAME(MODE_FEBRUUS, vector<string>({"feb","februus"}), FrameFebruus);

Coordinator *FrameFebruus::CreateCoordinator(cooid_t coo_id,
                                             Config *config,
                                             int benchmark,
                                             ClientControlServiceImpl *ccsi,
                                             uint32_t id,
                                             shared_ptr<TxnRegistry> txn_reg) {
  verify(config != nullptr);
  CoordinatorFebruus *coord = new CoordinatorFebruus(coo_id,
                                                     benchmark,
                                                     ccsi,
                                                     id);
  coord->txn_reg_ = txn_reg;
  coord->frame_ = this;
  return coord;
}

TxLogServer *FrameFebruus::CreateScheduler() {
  TxLogServer *sched = new SchedulerFebruus();
  sched->frame_ = this;
  return sched;
}
//
//vector<rrr::Service *>
//JanusFrame::CreateRpcServices(uint32_t site_id,
//                            Scheduler *sched,
//                            rrr::PollMgr *poll_mgr,
//                            ServerControlServiceImpl* scsi) {
//  return Frame::CreateRpcServices(site_id, sched, poll_mgr, scsi);
//}

mdb::Row *FrameFebruus::CreateRow(const mdb::Schema *schema,
                                  vector<Value> &row_data) {
  mdb::Row *r = mdb::VersionedRow::create(schema, row_data);
  return r;
}

shared_ptr<Tx> FrameFebruus::CreateTx(epoch_t epoch, txnid_t tid,
                                      bool ro, TxLogServer *mgr) {
  shared_ptr<Tx> sp_tx(new TxFebruus(epoch, tid, mgr));
  return sp_tx;
}

//Communicator* JanusFrame::CreateCommo(PollMgr* poll) {
//  return new JanusCommo(poll);
//}

} // namespace janus
