#include "../__dep__.h"
#include "../constants.h"
#include "frame.h"
#include "coordinator.h"
#include "scheduler.h"
#include "tx.h"
#include "commo.h"
#include "config.h"

namespace janus {

REG_FRAME(MODE_TAPIR, vector<string>({"tapir"}), FrameTapir);

Coordinator *FrameTapir::CreateCoordinator(cooid_t coo_id,
                                           Config *config,
                                           int benchmark,
                                           ClientControlServiceImpl *ccsi,
                                           uint32_t id,
                                           shared_ptr<TxnRegistry> txn_reg) {
  verify(config != nullptr);
  CoordinatorTapir *coord = new CoordinatorTapir(coo_id,
                                                 benchmark,
                                                 ccsi,
                                                 id);
  coord->txn_reg_ = txn_reg;
  coord->frame_ = this;
  return coord;
}

Communicator *FrameTapir::CreateCommo(PollMgr *pollmgr) {
  // Default: return null;
  commo_ = new TapirCommo(pollmgr);
  return commo_;
}

TxLogServer *FrameTapir::CreateScheduler() {
  TxLogServer *sched = new SchedulerTapir();
  sched->frame_ = this;
  return sched;
}

mdb::Row *FrameTapir::CreateRow(const mdb::Schema *schema,
                                vector<Value> &row_data) {

  mdb::Row *r = mdb::VersionedRow::create(schema, row_data);
  return r;
}

shared_ptr<Tx> FrameTapir::CreateTx(epoch_t epoch, txnid_t tid,
                                    bool ro, TxLogServer *mgr) {
  shared_ptr<Tx> sp_tx(new TxTapir(epoch, tid, mgr));
  return sp_tx;
}

} // namespace janus
