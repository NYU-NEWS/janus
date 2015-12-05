//
// Created by shuai on 11/25/15.
//
#pragma once

#include "../none/sched.h"

namespace rococo {

class ThreePhaseSched: public NoneSched {
  using NoneSched::NoneSched;
 public:
  void OnPhaseTwoRequest();
  void OnPhaseThreeRequest();
};

}