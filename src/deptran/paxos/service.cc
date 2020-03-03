
#include "service.h"
#include "server.h"

namespace janus {

MultiPaxosServiceImpl::MultiPaxosServiceImpl(TxLogServer *sched)
    : sched_((PaxosServer*)sched) {

}

void MultiPaxosServiceImpl::Forward(const MarshallDeputy& cmd,
                                    rrr::DeferredReply* defer) {
}

void MultiPaxosServiceImpl::Prepare(const uint64_t& slot,
                                    const ballot_t& ballot,
                                    ballot_t* max_ballot,
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
                                   ballot_t* max_ballot,
                                   rrr::DeferredReply* defer) {
  verify(sched_ != nullptr);
  sched_->OnAccept(slot,
                   ballot,
                   const_cast<MarshallDeputy&>(md_cmd).sp_data_,
                   max_ballot,
                   std::bind(&rrr::DeferredReply::reply, defer));
}

void MultiPaxosServiceImpl::Decide(const uint64_t& slot,
                                   const ballot_t& ballot,
                                   const MarshallDeputy& md_cmd,
                                   rrr::DeferredReply* defer) {
  verify(sched_ != nullptr);
  auto x = md_cmd.sp_data_;
  sched_->OnCommit(slot, ballot,x);
  defer->reply();
}


} // namespace janus;
