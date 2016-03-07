#pragma once

#include "../none/coord.h"

namespace rococo {

#define MAGIC_FACCEPT_BALLOT 1;
#define MAGIC_SACCEPT_BALLOT 2;

class TapirCommo;
class TapirCoord : public NoneCoord {
 public:
  enum Decision { UNKNOWN, COMMIT, ABORT };
  Decision decision_ = UNKNOWN;
  map<parid_t, int> n_accept_oks_ = {};
  map<parid_t, int> n_accpet_rejects_ = {};
  map<parid_t, int> n_fast_accept_oks_ = {};
  map<parid_t, int> n_fast_accept_rejects_ = {};
  using NoneCoord::NoneCoord;

//  void do_one(TxnRequest &) override;
  void Reset() override;
  TapirCommo* commo();

  void Handout() override;
  void HandoutAck(phase_t, int res, Command& cmd) override;

  void FastAccept();
  void FastAcceptAck(phase_t phase, parid_t par_id, Future *fu);

  bool AllFastQuorumReached();
  bool AllSlowQuorumReached();

  // either commit or abort.
  void Decide();

  void Accept();
  void AcceptAck(phase_t phase, parid_t par_id, Future *fu);

  void restart(TxnCommand *);

  int GetFastQuorum(parid_t par_id);
  int GetSlowQuorum(parid_t par_id);

  bool FastQuorumPossible();
};

} // namespace rococo