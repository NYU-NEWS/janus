#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../communicator.h"

namespace rococo {

class TxnCommand;
class MultiPaxosCommo : public Communicator {
 public:
  MultiPaxosCommo() = delete;
  MultiPaxosCommo(PollMgr*);
  void BroadcastPrepare(parid_t par_id,
                        slotid_t slot_id,
                        ballot_t ballot,
                        const function<void(Future *fu)> &callback);
  void BroadcastAccept(parid_t par_id,
                       slotid_t slot_id,
                       ballot_t ballot,
                       ContainerCommand& cmd,
                       const function<void(Future*)> &callback);
  void BroadcastDecide(const parid_t par_id,
                       const slotid_t slot_id,
                       const ballot_t ballot,
                       const ContainerCommand& cmd);
};

} // namespace rococo
