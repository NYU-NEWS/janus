//
// Created by shuai on 11/25/15.
//
#pragma once

#include "../scheduler.h"

namespace rococo {

class NoneSched: public Scheduler {
  using Scheduler::Scheduler;
 public:
  virtual int OnPhaseOneRequest(
      const RequestHeader &header,
      const std::vector<mdb::Value> &input,
      const rrr::i32 &output_size,
      rrr::i32 *res,
      std::vector<mdb::Value> *output,
      rrr::DeferredReply *defer);
};

}