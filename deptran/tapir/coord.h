#pragma once

#include "../coordinator.h"

class TapirCommo;
namespace rococo {

class TapirCoord : public Coordinator {
 public:
  using Coordinator::Coordinator;

  void do_one(TxnRequest &) override;

  TapirCommo* commo();

  void FastAccept();
  void FastAcceptAck(phase_t phase, parid_t par_id, Future *fu);

  // either commit or abort.
  void Decide();

  void restart(TxnCommand*) {verify(0);};
};

} // namespace rococo