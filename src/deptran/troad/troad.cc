
#include "../__dep__.h"
#include "../command.h"
#include "../command_marshaler.h"
#include "../communicator.h"
#include "../rcc/row.h"
#include "troad.h"
#include "tx.h"
#include "commo.h"
#include "coordinator.h"

namespace janus {

REG_FRAME(MODE_TROAD, vector<string>({"troad"}), TroadFrame);

Coordinator *TroadFrame::CreateCoordinator(cooid_t coo_id,
                                           Config *config,
                                           int benchmark,
                                           ClientControlServiceImpl *ccsi,
                                           uint32_t id,
                                           shared_ptr<TxnRegistry> txn_reg) {
  verify(config != nullptr);
  auto *coord = new CoordinatorTroad(coo_id,
                                                 benchmark,
                                                 ccsi,
                                                 id);
  coord->txn_reg_ = txn_reg;
  coord->frame_ = this;
  return coord;
}

Executor *TroadFrame::CreateExecutor(uint64_t, TxLogServer *sched) {
  verify(0);
  return nullptr;
}

TxLogServer *TroadFrame::CreateScheduler() {
  TxLogServer *sched = new SchedulerTroad();
  sched->frame_ = this;
  return sched;
}

vector<rrr::Service *>
TroadFrame::CreateRpcServices(uint32_t site_id,
                              TxLogServer *sched,
                              rrr::PollMgr *poll_mgr,
                              ServerControlServiceImpl *scsi) {
  return Frame::CreateRpcServices(site_id, sched, poll_mgr, scsi);
}

mdb::Row *TroadFrame::CreateRow(const mdb::Schema *schema,
                                vector<Value> &row_data) {

  mdb::Row *r = RccRow::create(schema, row_data);
  return r;
}

shared_ptr<Tx> TroadFrame::CreateTx(epoch_t epoch, txnid_t tid,
                                    bool ro, TxLogServer *mgr) {
//  auto dtxn = new JanusDTxn(tid, mgr, ro);
//  return dtxn;

  auto p = new TxTroad(epoch, tid, mgr, ro);
  shared_ptr<Tx> sp_tx(p);
  return sp_tx;
}

shared_ptr<Tx> TroadJanusFrame::CreateTx(epoch_t epoch, txnid_t tid,
                                    bool ro, TxLogServer *mgr) {
//  auto dtxn = new JanusDTxn(tid, mgr, ro);
//  return dtxn;

  auto p = new TxTroad(epoch, tid, mgr, ro);
  p->mocking_janus_ = true;
  shared_ptr<Tx> sp_tx(p);
  return sp_tx;
}

Coordinator *TroadJanusFrame::CreateCoordinator(cooid_t coo_id,
                                                Config *config,
                                                int benchmark,
                                                ClientControlServiceImpl *ccsi,
                                                uint32_t id,
                                                shared_ptr<TxnRegistry> txn_reg) {
  verify(config != nullptr);
  auto *coord = new CoordinatorTroad(coo_id,
                                     benchmark,
                                     ccsi,
                                     id);
  coord->txn_reg_ = txn_reg;
  coord->mocking_janus_ = true;
  coord->frame_ = this;
  return coord;
}

Communicator *TroadFrame::CreateCommo(PollMgr *poll) {
  return new TroadCommo(poll);
}

} // namespace janus
