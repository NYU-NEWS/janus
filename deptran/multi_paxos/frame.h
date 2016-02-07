#pragma once

#include <deptran/communicator.h>
#include "../frame.h"
#include "../constants.h"

namespace rococo {

class MultiPaxosFrame : public Frame {
 public:
  using Frame::Frame;
  Executor* CreateExecutor(cmdid_t cmd_id, Scheduler* sched) override;
  Coordinator* CreateCoord(cooid_t coo_id,
                           Config* config,
                           int benchmark,
                           ClientControlServiceImpl *ccsi,
                           uint32_t id,
                           bool batch_start,
                           TxnRegistry* txn_reg) override;
  Scheduler* CreateScheduler() override;
  Communicator* CreateCommo() override;
  vector<rrr::Service *> CreateRpcServices(uint32_t site_id,
                                           Scheduler *dtxn_sched,
                                           rrr::PollMgr *poll_mgr,
                                           ServerControlServiceImpl *scsi) override;
};


//class MultiPaxosDummy {
// public:
//  static MultiPaxosDummy* mpd_s;
//  MultiPaxosDummy() {
//    Frame* mpf = Frame::RegFrame(MODE_MULTI_PAXOS, new MultiPaxosFrame());
//    std::cout << "hello dummy multi paxos" << std::endl;
//  }
//};
//static MultiPaxosDummy dummy___;


} // namespace rococo
