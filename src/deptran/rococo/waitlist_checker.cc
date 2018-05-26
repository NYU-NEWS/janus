

#include "waitlist_checker.h"
#include "scheduler.h"

using namespace janus;

WaitlistChecker::WaitlistChecker(SchedulerRococo* sched) {
  this->set_period(2000000); // 2s
  verify(sched_ == nullptr);
  sched_ = sched;
}

void WaitlistChecker::Work() {
  sched_->CheckWaitlist();
}
