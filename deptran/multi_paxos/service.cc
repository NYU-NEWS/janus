
#include "service.h"

namespace rococo {

void MultiPaxosServiceImpl::Forward(const SimpleCommand& cmd,
                                    rrr::DeferredReply* defer) {

}

void MultiPaxosServiceImpl::Prepare(const uint64_t& slot,
                                    const ballot_t& ballot,
                                    uint64_t* max_ballot,
                                    rrr::DeferredReply* defer) {


}

void MultiPaxosServiceImpl::Accept(const uint64_t& slot,
                                   const ballot_t& ballot,
                                   const SimpleCommand& cmd,
                                   uint64_t* max_ballot,
                                   rrr::DeferredReply* defer) {

}

void MultiPaxosServiceImpl::Decide(const uint64_t& slot,
                                   const ballot_t& ballot,
                                   const SimpleCommand& cmd,
                                   rrr::DeferredReply* defer) {

}


} // namespace rococo;