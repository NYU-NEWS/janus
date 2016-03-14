#pragma once
#include "../__dep__.h"
#include "../communicator.h"

namespace rococo {

class RccGraph;
class RccCommo : public Communicator {
  void SendHandout(SimpleCommand &cmd,
                   Coordinator *coo,
                   const std::function<void(phase_t,
                                            int res,
                                            Command& cmd,
                                            RccGraph& graph)>&) ;
  void SendFinish(parid_t pid,
                  txnid_t tid,
                  const std::function<void(Future *fu)> &callback) ;
};

} // namespace rococo