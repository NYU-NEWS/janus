

#include "communicator.h"
#include "rcc/graph.h"
#include "rcc/graph_marshaler.h"
#include "command.h"
#include "command_marshaler.h"
#include "rcc_rpc.h"


namespace rococo {

Communicator::Communicator() {
  vector<string> addrs;
  rpc_poll_ = new PollMgr(1);
  auto config = Config::GetConfig();
  config->get_all_site_addr(addrs);
  vector<parid_t> partitions = config->GetAllPartitionIds();
  for (auto &par_id : partitions) {
    auto site_infos = config->SitesByPartitionId(par_id);
    vector<RococoProxy*> proxies;
    for (auto &si : site_infos) {
      string addr = si.GetHostAddr();
      rrr::Client *rpc_cli = new rrr::Client(rpc_poll_);
      Log::info("connect to site: %s", addr.c_str());
      auto ret = rpc_cli->connect(addr.c_str());
      verify(ret == 0);
      RococoProxy *rpc_proxy = new RococoProxy(rpc_cli);
      rpc_clients_.insert(std::make_pair(si.id, rpc_cli));
      rpc_proxies_.insert(std::make_pair(si.id, rpc_proxy));
      proxies.push_back(rpc_proxy);
    }
    rpc_par_proxies_.insert(std::make_pair(par_id, proxies));
  }

  verify(addrs.size() > 0);
  for (auto &addr : addrs) {
  }
}


} // namespace rococo