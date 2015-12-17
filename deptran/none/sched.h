//
// Created by shuai on 11/25/15.
//
#pragma once

#include "../scheduler.h"

namespace rococo {

class NoneSched: public Scheduler {
  using Scheduler::Scheduler;
 public:
  virtual int OnPhaseOneRequest(const RequestHeader &header,
                                const map<int32_t, Value> &input,
                                const rrr::i32 &output_size,
                                rrr::i32 *res,
                                map<int32_t, Value> *output,
                                rrr::DeferredReply *defer);
};

}