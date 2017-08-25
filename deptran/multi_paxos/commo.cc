
#include "commo.h"
#include "../rococo/graph.h"
#include "../rococo/graph_marshaler.h"
#include "../command.h"
#include "deptran/procedure.h"
#include "../command_marshaler.h"
#include "../rcc_rpc.h"

namespace rococo {

MultiPaxosCommo::MultiPaxosCommo(PollMgr* poll) : Communicator(poll) {
//  verify(poll != nullptr);
}

void MultiPaxosCommo::BroadcastPrepare(parid_t par_id,
                                       slotid_t slot_id,
                                       ballot_t ballot,
                                       const function<void(Future*)> &cb) {

  auto proxies = rpc_par_proxies_[par_id];
  vector<Future*> fus;
  for (auto &p : proxies) {
    auto proxy = (MultiPaxosProxy*) p.second;
    FutureAttr fuattr;
    fuattr.callback = cb;
    Future::safe_release(proxy->async_Prepare(slot_id, ballot, fuattr));
  }
}

void MultiPaxosCommo::BroadcastAccept(parid_t par_id,
                                      slotid_t slot_id,
                                      ballot_t ballot,
                                      ContainerCommand& cmd,
                                      const function<void(Future*)> &cb) {
  auto proxies = rpc_par_proxies_[par_id];
  vector<Future*> fus;
  for (auto &p : proxies) {
    auto proxy = (MultiPaxosProxy*) p.second;
    FutureAttr fuattr;
    fuattr.callback = cb;
    MarshallDeputy md(&cmd, false);
    Future::safe_release(proxy->async_Accept(slot_id,
                                             ballot,
                                             md,
                                             fuattr));
  }
//  verify(0);
}

void MultiPaxosCommo::BroadcastDecide(const parid_t par_id,
                                      const slotid_t slot_id,
                                      const ballot_t ballot,
                                      const ContainerCommand& cmd) {
  auto proxies = rpc_par_proxies_[par_id];
  vector<Future*> fus;
  for (auto &p : proxies) {
    auto proxy = (MultiPaxosProxy*) p.second;
    FutureAttr fuattr;
    fuattr.callback = [](Future* fu) {};
    ContainerCommand& tmp_cmd = const_cast<ContainerCommand&>(cmd);
    MarshallDeputy md(&tmp_cmd, false);
    Future::safe_release(proxy->async_Decide(slot_id,
                                             ballot,
                                             md,
                                             fuattr));
  }
}

} // namespace rococo
