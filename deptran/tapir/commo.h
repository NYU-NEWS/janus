#pragma once

#include "../communicator.h"

namespace rococo {

class TapirCommo : public Communicator {
 public:
  using Communicator::Communicator;
  void BroadcastFastAccept(SimpleCommand& cmd,
                           Coordinator* coord,
                           const function<void(Future* fu)>& callback);
};

}
