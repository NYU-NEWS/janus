#pragma once
#include "../__dep__.h"
#include "../communicator.h"

namespace rococo {

class RccGraph;
class RccCommo : public Communicator {
 public:
  void SendHandout(SimpleCommand &cmd,
                   Coordinator *coo,
                   const function<void(phase_t phase,
                                       int res,
                                       SimpleCommand& cmd,
                                       RccGraph& graph)>&) ;
  void SendFinish(parid_t pid,
                  txnid_t tid,
                  RccGraph& graph,
                  const function<void(Future *fu)> &callback) ;

  void SendInquire(parid_t pid,
                   txnid_t tid,
                   const function<void(RccGraph& graph)>&);
};

} // namespace rococo