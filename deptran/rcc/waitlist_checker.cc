

#include "waitlist_checker.h"
#include "sched.h"

using namespace rococo;

WaitlistChecker::WaitlistChecker(RccSched* sched) {
  this->set_period(2000000); // 2s
  verify(sched_ == nullptr);
  sched_ = sched;
}

void WaitlistChecker::run() {
  sched_->CheckWaitlist();
}