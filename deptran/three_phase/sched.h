//
// Created by shuai on 11/25/15.
//
#pragma once

#include "../none/sched.h"

namespace rococo {

class ThreePhaseSched: public NoneSched {
  using NoneSched::NoneSched;
 public:
  virtual int OnPhaseOneRequest(
      const RequestHeader &header,
      const std::vector<mdb::Value> &input,
      const rrr::i32 &output_size,
      rrr::i32 *res,
      std::vector<mdb::Value> *output,
      rrr::DeferredReply *defer);

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