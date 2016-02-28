

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
  vector<parid_t> partitions = config->GetAllPartitionIds();
  for (auto &par_id : partitions) {
    auto site_infos = config->SitesByPartitionId(par_id);
    vector<std::pair<siteid_t, RococoProxy*>> proxies;
    for (auto &si : site_infos) {
      string addr = si.GetHostAddr();
      rrr::Client *rpc_cli = new rrr::Client(rpc_poll_);
      Log::info("connect to site: %s", addr.c_str());
      auto ret = rpc_cli->connect(addr.c_str());
      verify(ret == 0);
      RococoProxy *rpc_proxy = new RococoProxy(rpc_cli);
      rpc_clients_.insert(std::make_pair(si.id, rpc_cli));
      rpc_proxies_.insert(std::make_pair(si.id, rpc_proxy));
      proxies.push_back(std::make_pair(si.id, rpc_proxy));
    }
    rpc_par_proxies_.insert(std::make_pair(par_id, proxies));
  }
}

std::pair<siteid_t, RococoProxy*> Communicator::RandomProxyForPartition(parid_t partition_id) const {
  auto it = rpc_par_proxies_.find(partition_id);
  verify(it != rpc_par_proxies_.end());
  auto& partition_proxies = it->second;
  int index = rrr::RandomGenerator::rand(0, partition_proxies.size()-1);
  auto site_id = partition_proxies[index].first;
  auto& proxy = partition_proxies[index].second;
  return std::make_pair(site_id, proxy);
}

} // namespace rococo