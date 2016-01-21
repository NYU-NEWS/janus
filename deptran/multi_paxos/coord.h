#pragma once

#include "../__dep__.h"
#include "../coordinator.h"

namespace rococo {

class MultiPaxosCoord : public Coordinator {
 public:
  using Coordinator::Coordinator;
  void do_one(TxnRequest &req) override {}

  void Prepare() {}
  void Accept() {}
  void Decide() {}

  void cleanup() override {}
  void restart(TxnChopper* ch) override {}
};

} //namespace rococo
