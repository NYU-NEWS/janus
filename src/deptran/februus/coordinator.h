#pragma once

#include "../classic/coordinator.h"

namespace janus {

class CommoFebruus;
class CoordinatorFebruus : public CoordinatorClassic {
 public:
  enum Phase {
    INIT_END = 0, DISPATCH = 1, PREPARE = 2,
    PRE_ACCEPT = 3, ACCEPT = 4, COMMIT = 5
  };
  bool fast_path_{false};
  using CoordinatorClassic::CoordinatorClassic;
  void DispatchAsync() override;
  void DispatchAck(phase_t phase, int ret, TxnOutput& outputs) override;
  virtual void GotoNextPhase() override;
  bool PreAccept();
  void Prepare();
  bool Accept();
  void Commit() override;

  CommoFebruus* commo();
};

} // namespace janus
