//
// Created by Shuai on 2/27/17.
//

#include "kvdb.h"
#include "dtxn.h"

#pragma once

namespace janus {

class Procedure {
 public:
};

class SubProcedure {
 public:
  virtual void run(AbstractKvDb& db, DTxn& dtxn) = 0;
};

} // namespace janus

