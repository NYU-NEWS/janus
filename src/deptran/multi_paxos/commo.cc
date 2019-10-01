
#include "commo.h"
#include "../rococo/graph.h"
#include "../rococo/graph_marshaler.h"
#include "../command.h"
#include "../procedure.h"
#include "../command_marshaler.h"
#include "../rcc_rpc.h"

namespace janus {

MultiPaxosCommo::MultiPaxosCommo(PollMgr* poll) : Communicator(poll) {
//  verify(poll != nullptr);
}

void MultiPaxosCommo::BroadcastPrepare(parid_t par_id,
                                       slotid_t slot_id,
                                       ballot_t ballot,
                                       const function<void(Future*)>& cb) {

  auto proxies = rpc_par_proxies_[par_id];
  for (auto& p : proxies) {
    auto proxy = (MultiPaxosProxy*) p.second;
    FutureAttr fuattr;
    fuattr.callback = cb;
    Future::safe_release(proxy->async_Prepare(slot_id, ballot, fuattr));
  }
}

void MultiPaxosCommo::BroadcastPrepare(shared_ptr<QuorumEvent> sp_quorum_event,
                                       parid_t par_id,
                                       slotid_t slot_id,
                                       ballot_t ballot) {
  auto proxies = rpc_par_proxies_[par_id];
  for (auto& p : proxies) {
    auto proxy = (MultiPaxosProxy*) p.second;
    FutureAttr fuattr;
    fuattr.callback = [sp_quorum_event, ballot](Future* fu) {
      ballot_t b = 0;
      fu->get_reply() >> b;
      if (b == ballot) {
        sp_quorum_event->n_voted_++;
        sp_quorum_event->Test();
      }
      // TODO process the case if all prepares are rejected.
    };
    Future::safe_release(proxy->async_Prepare(slot_id, ballot, fuattr));
  }

}

void MultiPaxosCommo::BroadcastAccept(shared_ptr<QuorumEvent> sp_quorum_event,
                                      parid_t par_id,
                                      slotid_t slot_id,
                                      ballot_t ballot,
                                      shared_ptr<Marshallable> cmd) {
  auto proxies = rpc_par_proxies_[par_id];
  vector<Future*> fus;
  for (auto& p : proxies) {
    auto proxy = (MultiPaxosProxy*) p.second;
    FutureAttr fuattr;
    fuattr.callback = [sp_quorum_event, ballot] (Future* fu) {
      ballot_t b = 0;
      fu->get_reply() >> b;
      if (b == ballot) {
        sp_quorum_event->n_voted_++;
        sp_quorum_event->Test();
      }
    };
    MarshallDeputy md(cmd);
    auto f = proxy->async_Accept(slot_id, ballot, md, fuattr);
    Future::safe_release(f);
  }
//  verify(0);
}

void MultiPaxosCommo::BroadcastAccept(parid_t par_id,
                                      slotid_t slot_id,
                                      ballot_t ballot,
                                      shared_ptr<Marshallable> cmd,
                                      const function<void(Future*)>& cb) {
  auto proxies = rpc_par_proxies_[par_id];
  vector<Future*> fus;
  for (auto& p : proxies) {
    auto proxy = (MultiPaxosProxy*) p.second;
    FutureAttr fuattr;
    fuattr.callback = cb;
    MarshallDeputy md(cmd);
    auto f = proxy->async_Accept(slot_id, ballot, md, fuattr);
    Future::safe_release(f);
  }
//  verify(0);
}

void MultiPaxosCommo::BroadcastDecide(const parid_t par_id,
                                      const slotid_t slot_id,
                                      const ballot_t ballot,
                                      const shared_ptr<Marshallable> cmd) {
  auto proxies = rpc_par_proxies_[par_id];
  vector<Future*> fus;
  for (auto& p : proxies) {
    auto proxy = (MultiPaxosProxy*) p.second;
    FutureAttr fuattr;
    fuattr.callback = [](Future* fu) {};
    MarshallDeputy md(cmd);
    auto f = proxy->async_Decide(slot_id, ballot, md, fuattr);
    Future::safe_release(f);
  }
}

} // namespace janus
