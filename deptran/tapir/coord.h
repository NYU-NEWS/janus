#pragma once

#include "../coordinator.h"

class TapirCommo;
namespace rococo {

class TapirCoord : public Coordinator {
 public:
  enum Decision {UNKNOWN, COMMIT, ABORT};
  Decision decision_ = UNKNOWN;
  map<parid_t, int> n_fast_accept_oks_ = {};
  using Coordinator::Coordinator;

  void do_one(TxnRequest &) override;
  void Reset() override;
  TapirCommo* commo();

  void FastAccept();
  void FastAcceptAck(phase_t phase, parid_t par_id, Future *fu);
  bool CheckAllFastQuorum();
  // either commit or abort.
  void Decide();

  void restart(TxnCommand*) {verify(0);};

  int GetFastQuorum(parid_t par_id);
};

} // namespace rococo