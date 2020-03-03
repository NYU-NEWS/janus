#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../communicator.h"

namespace janus {

class TxData;

class PaxosPrepareQuorumEvent: public QuorumEvent {
 public:
  using QuorumEvent::QuorumEvent;
//  ballot_t max_ballot_{0};
  bool HasAcceptedValue() {
    // TODO implement this
    return false;
  }
  void FeedResponse(bool y) {
    if (y) {
      VoteYes();
    } else {
      VoteNo();
    }
  }


};

class PaxosAcceptQuorumEvent: public QuorumEvent {
 public:
  using QuorumEvent::QuorumEvent;
  void FeedResponse(bool y) {
    if (y) {
      VoteYes();
    } else {
      VoteNo();
    }
  }
};

class MultiPaxosCommo : public Communicator {
 public:
  MultiPaxosCommo() = delete;
  MultiPaxosCommo(PollMgr*);
  shared_ptr<PaxosPrepareQuorumEvent>
  BroadcastPrepare(parid_t par_id,
                   slotid_t slot_id,
                   ballot_t ballot);
  void BroadcastPrepare(parid_t par_id,
                        slotid_t slot_id,
                        ballot_t ballot,
                        const function<void(Future *fu)> &callback);
  shared_ptr<PaxosAcceptQuorumEvent>
  BroadcastAccept(parid_t par_id,
                  slotid_t slot_id,
                  ballot_t ballot,
                  shared_ptr<Marshallable> cmd);
  void BroadcastAccept(parid_t par_id,
                       slotid_t slot_id,
                       ballot_t ballot,
                       shared_ptr<Marshallable> cmd,
                       const function<void(Future*)> &callback);
  void BroadcastDecide(const parid_t par_id,
                       const slotid_t slot_id,
                       const ballot_t ballot,
                       const shared_ptr<Marshallable> cmd);
};

} // namespace janus
