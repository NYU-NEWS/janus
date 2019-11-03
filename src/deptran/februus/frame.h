#pragma once

#include "../frame.h"

namespace janus {

class FrameFebruus : public Frame {
 public:
  FrameFebruus(int mode = MODE_FEBRUUS) : Frame(mode) {}
  TxLogServer *CreateScheduler() override;
//  Executor *CreateExecutor(cmdid_t, Scheduler *sched) override;
  Coordinator *CreateCoordinator(cooid_t coo_id,
                                 Config *config,
                                 int benchmark,
                                 ClientControlServiceImpl *ccsi,
                                 uint32_t id,
                                 shared_ptr<TxnRegistry> txn_reg) override;
//  vector<rrr::Service *> CreateRpcServices(uint32_t site_id,
//                                           Scheduler *dtxn_sched,
//                                           rrr::PollMgr *poll_mgr,
//                                           ServerControlServiceImpl *scsi)
//      override;
  mdb::Row *CreateRow(const mdb::Schema *schema,
                      vector<Value> &row_data) override;

  shared_ptr<Tx> CreateTx(epoch_t epoch,
                          txid_t tx_id,
                          bool ro,
                          TxLogServer *mgr) override;

//  Communicator* CreateCommo(PollMgr* poll = nullptr) override;
};

} // namespace janus
