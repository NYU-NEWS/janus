
#pragma once

#include "commo.h"

namespace rococo {

void RccCommo::SendHandout(SimpleCommand &cmd,
                           Coordinator *coo,
                           const function<void(phase_t phase,
                                               int res,
                                               SimpleCommand& cmd,
                                               RccGraph& graph)>&) {
  verify(0);
}
void RccCommo::SendFinish(parid_t pid,
                          txnid_t tid,
                          RccGraph& graph,
                          const function<void(Future *fu)> &callback) {
  verify(0);
}

void RccCommo::SendInquire(parid_t pid,
                           txnid_t tid,
                           const function<void(RccGraph& graph)>&) {
  verify(0);
}


} // namespace rococo