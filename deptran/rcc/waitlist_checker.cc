

#include "waitlist_checker.h"
#include "sched.h"

using namespace rococo;

WaitlistChecker::WaitlistChecker(RccSched* sched) {
//  this->set_period(500000); // 0.5s
  this->set_period(50); // 0.5s
  verify(sched_ == nullptr);
  sched_ = sched;
}

void WaitlistChecker::run() {
  sched_->CheckWaitlist();
}