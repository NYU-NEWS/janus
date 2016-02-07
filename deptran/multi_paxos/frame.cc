#include "../__dep__.h"
#include "../constants.h"
#include "frame.h"
#include "exec.h"
#include "coord.h"
#include "sched.h"
#include "service.h"
#include "commo.h"

namespace rococo {

static Frame* mpf = Frame::RegFrame(MODE_MULTI_PAXOS, new MultiPaxosFrame(MODE_MULTI_PAXOS));

//MultiPaxosDummy* MultiPaxosDummy::mpd_s = new MultiPaxosDummy();
//static const MultiPaxosDummy mpd_g;

Executor* MultiPaxosFrame::CreateExecutor(cmdid_t cmd_id, Scheduler* sched) {
  Executor* exec = new MultiPaxosExecutor(cmd_id, sched);
  return exec;
}

Coordinator* MultiPaxosFrame::CreateCoord(cooid_t coo_id,
                                          Config* config,
                                          int benchmark,
                                          ClientControlServiceImpl *ccsi,
                                          uint32_t id,
                                          bool batch_start,
                                          TxnRegistry* txn_reg) {
  Coordinator *coo;
  coo = new MultiPaxosCoord(coo_id,
                            benchmark,
                            ccsi,
                            id,
                            batch_start);
  return coo;
}

Scheduler* MultiPaxosFrame::CreateScheduler() {
  Scheduler *sch = nullptr;
  sch = new MultiPaxosSched();
  sch->frame_ = this;
  return sch;
}

Communicator* MultiPaxosFrame::CreateCommo() {
  return new MultiPaxosCommo();
}


vector<rrr::Service *>
MultiPaxosFrame::CreateRpcServices(uint32_t site_id,
                                   Scheduler *rep_sched,
                                   rrr::PollMgr *poll_mgr,
                                   ServerControlServiceImpl *scsi) {
  auto config = Config::GetConfig();
  auto result = std::vector<Service *>();
  switch(config->ab_mode_) {
    case MODE_MULTI_PAXOS:
      result.push_back(new MultiPaxosServiceImpl(rep_sched));
    default:
      break;
  }
  return result;
}

} // namespace rococo;
