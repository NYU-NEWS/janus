#include "../__dep__.h"
#include "../constants.h"
#include "frame.h"
#include "exec.h"
#include "coordinator.h"
#include "scheduler.h"
#include "service.h"
#include "commo.h"
#include "config.h"

namespace janus {

static Frame *mpf = Frame::RegFrame(MODE_MULTI_PAXOS,
                                    []() -> Frame * {
                                      return new MultiPaxosFrame
                                          (MODE_MULTI_PAXOS);
                                    });

MultiPaxosFrame::MultiPaxosFrame(int mode) : Frame(mode) {

}

Executor *MultiPaxosFrame::CreateExecutor(cmdid_t cmd_id, Scheduler *sched) {
  Executor *exec = new MultiPaxosExecutor(cmd_id, sched);
  return exec;
}

Coordinator *MultiPaxosFrame::CreateCoordinator(cooid_t coo_id,
                                                Config *config,
                                                int benchmark,
                                                ClientControlServiceImpl *ccsi,
                                                uint32_t id,
                                                TxnRegistry *txn_reg) {
  verify(config != nullptr);
  CoordinatorMultiPaxos *coo;
  coo = new CoordinatorMultiPaxos(coo_id,
                                  benchmark,
                                  ccsi,
                                  id);
  coo->frame_ = this;
  verify(commo_ != nullptr);
  coo->commo_ = commo_;
  coo->slot_hint_ = &slot_hint_;
  coo->slot_id_ = slot_hint_++;
  coo->n_replica_ = config->GetPartitionSize(site_info_->partition_id_);
  coo->loc_id_ = this->site_info_->locale_id;
  verify(coo->n_replica_ != 0); // TODO
  Log_debug("create new multi-paxos coord, slot_id: %d", (int) coo->slot_id_);
  return coo;
}

Scheduler *MultiPaxosFrame::CreateScheduler() {
  Scheduler *sch = nullptr;
  sch = new SchedulerMultiPaxos();
  sch->frame_ = this;
  return sch;
}

Communicator *MultiPaxosFrame::CreateCommo(PollMgr *poll) {
  // We only have 1 instance of MultiPaxosFrame object that is returned from
  // GetFrame method. MultiPaxosCommo currently seems ok to share among the
  // clients of this method.
  if (commo_ == nullptr) {
    commo_ = new MultiPaxosCommo(poll);
  }
  return commo_;
}

vector<rrr::Service *>
MultiPaxosFrame::CreateRpcServices(uint32_t site_id,
                                   Scheduler *rep_sched,
                                   rrr::PollMgr *poll_mgr,
                                   ServerControlServiceImpl *scsi) {
  auto config = Config::GetConfig();
  auto result = std::vector<Service *>();
  switch (config->replica_proto_) {
    case MODE_MULTI_PAXOS:result.push_back(new MultiPaxosServiceImpl(rep_sched));
    default:break;
  }
  return result;
}

} // namespace janus;
