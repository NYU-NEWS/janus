#include "service.h"
#include "sched.h"

namespace rococo {

TapirServiceImpl::TapirServiceImpl(Scheduler *sched) {
  sched_ = (TapirSched*)sched;
}

void TapirServiceImpl::Handout(const SimpleCommand& cmd,
                               rrr::i32* res,
                               map<int32_t, Value>* output,
                               rrr::DeferredReply* defer) {
  sched_->OnHandoutRequest(cmd, res, output, [defer] () {defer->reply();});
}

void TapirServiceImpl::Prepare(rrr::DeferredReply* defer) {
  verify(0);
}

void TapirServiceImpl::Accept(const cmdid_t& cmd_id,
                              const ballot_t& ballot,
                              const int32_t& decision,
                              rrr::DeferredReply* defer) {
  verify(0);
}

void TapirServiceImpl::FastAccept(const SimpleCommand& cmd,
                                  rrr::i32* res,
                                  std::map<rrr::i32, Value>* output,
                                  rrr::DeferredReply* defer) {
  // TODO
  sched_->OnFastAccept(cmd.root_id_, res, [defer] () {defer->reply();});
//  verify(0);
}

void TapirServiceImpl::Decide(const rrr::i64& cmd_id,
                              const rrr::i32& decision,
                              rrr::DeferredReply* defer) {
  // TODO
  defer->reply();
  verify(0);
}

} // namespace rococo