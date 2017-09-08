
#include "service.h"
#include "scheduler.h"

namespace janus {

MultiPaxosServiceImpl::MultiPaxosServiceImpl(Scheduler *sched)
    : sched_((SchedulerMultiPaxos*)sched) {

}

void MultiPaxosServiceImpl::Forward(const MarshallDeputy& cmd,
                                    rrr::DeferredReply* defer) {
}

void MultiPaxosServiceImpl::Prepare(const uint64_t& slot,
                                    const ballot_t& ballot,
                                    uint64_t* max_ballot,
                                    rrr::DeferredReply* defer) {
  verify(sched_ != nullptr);
  sched_->OnPrepare(slot,
                    ballot,
                    max_ballot,
                    std::bind(&rrr::DeferredReply::reply, defer));
}

void MultiPaxosServiceImpl::Accept(const uint64_t& slot,
                                   const ballot_t& ballot,
                                   const MarshallDeputy& md_cmd,
                                   uint64_t* max_ballot,
                                   rrr::DeferredReply* defer) {
  verify(sched_ != nullptr);
  auto& cmd = dynamic_cast<TxData&>(*md_cmd.sp_data_);
  sched_->OnAccept(slot,
                   ballot,
                   cmd,
                   max_ballot,
                   std::bind(&rrr::DeferredReply::reply, defer));
}

void MultiPaxosServiceImpl::Decide(const uint64_t& slot,
                                   const ballot_t& ballot,
                                   const MarshallDeputy& md_cmd,
                                   rrr::DeferredReply* defer) {
  verify(sched_ != nullptr);
  auto& cmd = dynamic_cast<TxData&>(*md_cmd.sp_data_);
  sched_->OnCommit(slot,
                   ballot,
                   cmd);
  defer->reply();
}


} // namespace janus;
