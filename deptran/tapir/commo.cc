
#include "command.h"
#include "command_marshaler.h"3
#include "tapir_srpc.h"
#include "commo.h"
#include "../coordinator.h"

namespace rococo {

void TapirCommo::SendHandout(SimpleCommand &cmd,
                             Coordinator *coo,
                             const function<void(int,
                                                 Command&)> &callback) {
  rrr::FutureAttr fuattr;
  parid_t par_id = cmd.PartitionId();
  auto proxy = (TapirProxy*) rpc_par_proxies_[par_id][0].second;
  std::function<void(Future*)> cb =
      [coo, this, callback, &cmd] (Future *fu) {
        Log_debug("SendStart callback for %ld from %ld",
                  cmd.PartitionId(),
                  coo->coo_id_);

        int res;
        Marshal &m = fu->get_reply();
        m >> res >> cmd.output;
        callback(res, cmd);
      };
  fuattr.callback = cb;
  Log_debug("SendStart to %ld from %ld", cmd.PartitionId(), coo->coo_id_);
  verify(cmd.type_ > 0);
  verify(cmd.root_type_ > 0);
  Future::safe_release(proxy->async_Handout(cmd, fuattr));
}

void TapirCommo::BroadcastFastAccept(SimpleCommand& cmd,
                                     const function<void(Future* fu)>& cb) {
  parid_t par_id = cmd.PartitionId();
  auto proxies = rpc_par_proxies_[par_id];
  for (auto &p : proxies) {
    auto proxy = (TapirProxy*) p.second;
    FutureAttr fuattr;
    fuattr.callback = cb;
    Future::safe_release(proxy->async_FastAccept(cmd, fuattr));
  }
}

void TapirCommo::BroadcastDecide(parid_t par_id,
                                 cmdid_t cmd_id,
                                 int decision) {
  auto proxies = rpc_par_proxies_[par_id];
  for (auto &p : proxies) {
    auto proxy = (TapirProxy*) p.second;
    FutureAttr fuattr;
    fuattr.callback = [] (Future* fu) {} ;
    Future::safe_release(proxy->async_Decide(cmd_id, decision, fuattr));
  }
}

void TapirCommo::BroadcastAccept(parid_t par_id,
                                 cmdid_t cmd_id,
                                 ballot_t ballot,
                                 int decision,
                                 const function<void(Future*)>& callback) {
  auto proxies = rpc_par_proxies_[par_id];
  for (auto &p: proxies) {
    auto proxy = (TapirProxy*) p.second;
    FutureAttr fuattr;
    fuattr.callback = callback;
    Future::safe_release(proxy->async_Accept(cmd_id,
                                             ballot,
                                             decision,
                                             fuattr));
  }
}


} // namespace rococo