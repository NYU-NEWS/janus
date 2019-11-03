#pragma once

#include "../frame.h"

namespace janus {

class JanusFrame : public Frame {
 public:
  JanusFrame(int mode = MODE_JANUS) : Frame(mode) {}
  Executor *CreateExecutor(cmdid_t, TxLogServer *sched) override;
  Coordinator *CreateCoordinator(cooid_t coo_id,
                                 Config *config,
                                 int benchmark,
                                 ClientControlServiceImpl *ccsi,
                                 uint32_t id,
                                 shared_ptr<TxnRegistry> txn_reg) override;
  TxLogServer *CreateScheduler() override;
  vector<rrr::Service *> CreateRpcServices(uint32_t site_id,
                                           TxLogServer *dtxn_sched,
                                           rrr::PollMgr *poll_mgr,
                                           ServerControlServiceImpl *scsi)
  override;
  mdb::Row *CreateRow(const mdb::Schema *schema,
                      vector<Value> &row_data) override;

  shared_ptr<Tx> CreateTx(epoch_t epoch, txnid_t tid,
                          bool ro, TxLogServer *mgr) override;

  Communicator *CreateCommo(PollMgr *poll = nullptr) override;
};

} // namespace janus
