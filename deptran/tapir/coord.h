#pragma once

#include "../coordinator.h"

class TapirCommo;
namespace rococo {

class TapirCoord : public Coordinator {
 public:
  using Coordinator::Coordinator;

  void do_one(TxnRequest &) override;

  TapirCommo* commo() {
    verify(0);
    return nullptr;
  };

  void FastAccept();
  void FastAcceptAck();

  // either commit or abort.
  void Decide();

  void restart(TxnCommand*) {verify(0);};
};

} // namespace rococo