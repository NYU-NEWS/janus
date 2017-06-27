//
// Created by Shuai on 6/25/17.
//

#include "frame.h"
#include "sched.h"

using namespace janus;

Scheduler* ExternCFrame::CreateScheduler() {
  Scheduler* sched = new ExternCScheduler();
  sched->frame_ = this;
  return sched;
}

