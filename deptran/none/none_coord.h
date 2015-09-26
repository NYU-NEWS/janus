#pragma once

#include "coordinator.h"

namespace rococo {
class NoneCoord : public Coordinator {
  using Coordinator::Coordinator;

 public:
  virtual void do_one(TxnRequest&);
  void LegacyStart(TxnChopper *ch);

};
} // namespace rococo
