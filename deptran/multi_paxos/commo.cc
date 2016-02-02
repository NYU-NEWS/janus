
#include "commo.h"
#include "../rcc/graph.h"
#include "../rcc/graph_marshaler.h"
#include "../command.h"
#include "../command_marshaler.h"
#include "../rcc_rpc.h"

namespace rococo {


void MultiPaxosCommo::BroadcastPrepare(parid_t par_id,
                                       slotid_t slot_id,
                                       ballot_t ballot,
                                       const function<void(Future*)> &cb) {

  auto proxies = (vector<MultiPaxosProxy*>)(rpc_par_proxies_[par_id]);
  vector<Future*> fus;
  for (auto &proxy : proxies) {
    FutureAttr fuattr;
    fuattr.callback = cb;
    Future::safe_release(proxy->async_Prepare(slot_id, ballot, fuattr));
  }
}

void MultiPaxosCommo::BroadcastAccept(parid_t par_id,
                                      slotid_t slot_id,
                                      ballot_t ballot,
                                      TxnCommand& cmd,
                                      const function<void(Future*)> &cb) {
  verify(0);
}

} // namespace rococo
