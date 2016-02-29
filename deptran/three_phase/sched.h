//
// Created by shuai on 11/25/15.
//
#pragma once

#include "../none/sched.h"

namespace rococo {

class ThreePhaseSched: public NoneSched {
 using NoneSched::NoneSched;
 public:

  virtual int OnExchangeRequest(const SimpleCommand &cmd,
                                rrr::i32 *res,
                                map<int32_t, Value> *output,
                                rrr::DeferredReply *defer);
  // PrepareRequest
  virtual int OnPhaseTwoRequest(cmdid_t cmd_id,
                        const std::vector <i32> &sids,
                        rrr::i32 *res,
                        rrr::DeferredReply *defer);
  virtual int OnPhaseThreeRequest(cmdid_t cmd_id,
                           int commit_or_abort,
                           rrr::i32 *res,
                           rrr::DeferredReply *defer);
};

}