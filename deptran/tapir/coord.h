#pragma once

#include "../coordinator.h"

class TapirCommo;
namespace rococo {

#define MAGIC_FACCEPT_BALLOT 1;
#define MAGIC_SACCEPT_BALLOT 2;

class TapirCoord : public Coordinator {
 public:
  enum Decision { UNKNOWN, COMMIT, ABORT };
  Decision decision_ = UNKNOWN;
  map<parid_t, int> n_accept_oks_ = {};
  map<parid_t, int> n_fast_accept_oks_ = {};
  using Coordinator::Coordinator;

  void do_one(TxnRequest &) override;
  void Reset() override;
  TapirCommo *commo();

  void FastAccept();
  void FastAcceptAck(phase_t phase, parid_t par_id, Future *fu);

  bool AllFastQuorumReached();
  bool AllSlowQuorumReached();

  // either commit or abort.
  void Decide();

  void Accept();
  void AcceptAck(phase_t phase, parid_t par_id, Future *fu);

  void restart(TxnCommand *) { verify(0); };

  int GetFastQuorum(parid_t par_id);
  int GetSlowQuorum(parid_t par_id);

  bool FastQuorumPossible();
};

} // namespace rococo