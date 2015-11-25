#pragma once

#include "marshal-value.h"
#include "coordinator.h"

namespace rococo {
class NoneCoord : public Coordinator {
  using Coordinator::Coordinator;

 public:
  virtual void do_one(TxnRequest&);
  void LegacyStartAck(TxnChopper *ch, int pi, Future *fu);
};
} // namespace rococo
