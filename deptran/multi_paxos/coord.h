#pragma once

#include "../__dep__.h"
#include "../coordinator.h"

namespace rococo {

class MultiPaxosCoord : public Coordinator {
 public:
  using Coordinator::Coordinator;
  void do_one(TxnRequest &req) override {}
  void Submit(SimpleCommand& cmd, std::function<void()> func) override;

  void Prepare() {}
  void Accept() {}
  void Decide() {}

  void cleanup() override {}
  void restart(TxnCommand * ch) override {}
};

} //namespace rococo
