#include "service.h"
#include "sched.h"

namespace rococo {

TapirServiceImpl::TapirServiceImpl(Scheduler *sched) {
  sched_ = (TapirSched*)sched;
}

void TapirServiceImpl::Prepare(rrr::DeferredReply* defer) {
  verify(0);
}

void TapirServiceImpl::Accept(rrr::DeferredReply* defer) {
  verify(0);
}

void TapirServiceImpl::FastAccept(const SimpleCommand& cmd,
                                  rrr::i32* res,
                                  std::map<rrr::i32, Value>* output,
                                  rrr::DeferredReply* defer) {
  // TODO
  *res = SUCCESS;
  defer->reply();
//  verify(0);
}

void TapirServiceImpl::Decide(const rrr::i64& cmd_id,
                              const rrr::i32& commit,
                              rrr::DeferredReply* defer) {
  // TODO
  defer->reply();
//  verify(0);
}

} // namespace rococo