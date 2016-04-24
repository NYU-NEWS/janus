
#include "service.h"
#include "sched.h"

namespace rococo {

MultiPaxosServiceImpl::MultiPaxosServiceImpl(Scheduler *sched)
    : sched_((MultiPaxosSched*)sched) {

}

void MultiPaxosServiceImpl::Forward(const ContainerCommand& cmd,
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
                                   const ContainerCommand& cmd,
                                   uint64_t* max_ballot,
                                   rrr::DeferredReply* defer) {
  verify(sched_ != nullptr);
  sched_->OnAccept(slot,
                   ballot,
                   cmd,
                   max_ballot,
                   std::bind(&rrr::DeferredReply::reply, defer));
}

void MultiPaxosServiceImpl::Decide(const uint64_t& slot,
                                   const ballot_t& ballot,
                                   const ContainerCommand& cmd,
                                   rrr::DeferredReply* defer) {
  verify(sched_ != nullptr);
  sched_->OnCommit(slot,
                   ballot,
                   cmd);
  defer->reply();
}


} // namespace rococo;
