//
// Created by chance_Lv on 2020/2/10.
//
#pragma once

#include <deptran/frame.h>

namespace janus {
   class FrameAcc : public Frame {
   public:
       explicit FrameAcc(int mode = MODE_ACC) : Frame(MODE_ACC) {}
       Coordinator* CreateCoordinator(cooid_t coo_id,
                                      Config *config,
                                      int benchmark,
                                      ClientControlServiceImpl *ccsi,
                                      uint32_t id,
                                      shared_ptr<TxnRegistry> txn_reg) override;
       TxLogServer* CreateScheduler() override;
       mdb::Row* CreateRow(const mdb::Schema *schema,
                           vector<Value> &row_data) override;
       shared_ptr<Tx> CreateTx(epoch_t epoch, txnid_t tid,
                               bool ro, TxLogServer *mgr) override;
       Communicator* CreateCommo(PollMgr *pollmgr) override;
   };
}