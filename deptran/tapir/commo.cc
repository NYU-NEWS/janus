
#include "command.h"
#include "command_marshaler.h"3
#include "tapir_srpc.h"
#include "commo.h"

namespace rococo {

void TapirCommo::BroadcastFastAccept(SimpleCommand& cmd,
                                     Coordinator* coord,
                                     const function<void(Future* fu)>& cb) {
  parid_t par_id = cmd.GetParId();
  auto proxies = rpc_par_proxies_[par_id];
  vector<Future*> fus;
  for (auto &p : proxies) {
    auto proxy = (TapirProxy*) p;
    FutureAttr fuattr;
    fuattr.callback = cb;
    Future::safe_release(proxy->async_FastAccept(cmd, fuattr));
  }
}

} // namespace rococo