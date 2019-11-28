#pragma once

#include "../__dep__.h"
#include "../frame.h"
#include "../constants.h"
#include "commo.h"

namespace janus {

class FrameTapir : public Frame {
 public:
  FrameTapir(int m=MODE_TAPIR) : Frame(MODE_TAPIR) {}
  Coordinator *CreateCoordinator(cooid_t coo_id,
                                 Config *config,
                                 int benchmark,
                                 ClientControlServiceImpl *ccsi,
                                 uint32_t id,
                                 shared_ptr<TxnRegistry>) override;
  TxLogServer *CreateScheduler() override;
  mdb::Row *CreateRow(const mdb::Schema *schema,
                      vector<Value> &row_data) override;
  Communicator *CreateCommo(PollMgr *pollmgr = nullptr) override;

  shared_ptr<Tx> CreateTx(epoch_t epoch, txnid_t tid,
                          bool ro, TxLogServer *mgr) override;

};
} // namespace janus
