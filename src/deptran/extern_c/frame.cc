//
// Created by Shuai on 6/25/17.
//

#include "frame.h"
#include "sched.h"

using namespace janus;

//static volatile Frame* externc_frame_s = Frame::RegFrame(MODE_EXTERNC,
//                                              {"externc", "extern_c"},
//                                              [] () -> Frame* {
//                                                return new ExternCFrame();
//                                              });

TxLogServer* ExternCFrame::CreateScheduler() {
  TxLogServer* sched = new ExternCScheduler();
  sched->frame_ = this;
  return sched;
}

