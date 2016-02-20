
#include "command.h"
#include "command_marshaler.h"3
#include "tapir_srpc.h"
#include "commo.h"

namespace rococo {

void TapirCommo::BroadcastFastAccept(SimpleCommand& cmd,
                                     const function<void(Future* fu)>& cb) {
  parid_t par_id = cmd.GetParId();
  auto proxies = rpc_par_proxies_[par_id];
  for (auto &p : proxies) {
    auto proxy = (TapirProxy*) p;
    FutureAttr fuattr;
    fuattr.callback = cb;
    Future::safe_release(proxy->async_FastAccept(cmd, fuattr));
  }
}

void TapirCommo::BroadcastDecide(parid_t par_id, cmdid_t cmd_id, int decision) {
  auto proxies = rpc_par_proxies_[par_id];
  for (auto &p : proxies) {
    auto proxy = (TapirProxy*) p;
    FutureAttr fuattr;
    fuattr.callback = [] (Future* fu) {} ;
    Future::safe_release(proxy->async_Decide(cmd_id, decision, fuattr));
  }
}


} // namespace rococo