#pragma once

#include "../none/coordinator.h"

namespace janus {

#define MAGIC_FACCEPT_BALLOT 1;
#define MAGIC_SACCEPT_BALLOT 2;

class TapirCommo;
class CoordinatorTapir : public CoordinatorClassic {
 public:
  enum Phase {INIT_END=0, DISPATCH=1, FAST_ACCEPT=2, DECIDE=3};
  enum Decision { UNKNOWN = 0, COMMIT = 1, ABORT = 2};
  map<parid_t, int> n_accept_oks_ = {};
  map<parid_t, int> n_accpet_rejects_ = {};
  map<parid_t, int> n_fast_accept_oks_ = {};
  map<parid_t, int> n_fast_accept_rejects_ = {};
  using CoordinatorClassic::CoordinatorClassic;

//  void do_one(TxnRequest &) override;
  void Reset() override;
  TapirCommo* commo();

  void DispatchAsync() override;
  void DispatchAck(phase_t, int32_t res, TxnOutput& output) override;

  void FastAccept();
  void FastAcceptAck(phase_t phase, parid_t par_id, int32_t res);

  bool AllFastQuorumReached();
  bool AllSlowQuorumReached();

  // either commit or abort.
  void Decide();

  void Accept();
  void AcceptAck(phase_t phase, parid_t par_id, Future *fu);

  void Restart() override;
  void GotoNextPhase() override;

  int GetFastQuorum(parid_t par_id);
  int GetSlowQuorum(parid_t par_id);

  bool FastQuorumPossible();
};

} // namespace janus
