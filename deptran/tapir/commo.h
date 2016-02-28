#pragma once

#include "../communicator.h"

namespace rococo {

class TapirCommo : public Communicator {
 public:
  using Communicator::Communicator;
  void BroadcastFastAccept(SimpleCommand& cmd,
                           const function<void(Future* fu)>& callback);
  void BroadcastDecide(parid_t,
                       cmdid_t cmd_id,
                       int decision);
  void BroadcastAccept(parid_t,
                       cmdid_t,
                       ballot_t,
                       int decision,
                       const function<void(Future*)>&);
};

}
