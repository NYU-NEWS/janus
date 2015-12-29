//
// Created by lamont on 12/28/15.
//
#pragma once
#include "deptran/sharding.h"

namespace rococo {
  class SimpleBenchSharding : public Sharding {
  public:
    virtual int PopulateTable(uint32_t sid) override;
  };
}
