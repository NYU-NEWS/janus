

#include "communicator.h"
#include "rcc/graph.h"
#include "rcc/graph_marshaler.h"
#include "command.h"
#include "command_marshaler.h"
#include "rcc_rpc.h"


namespace rococo {

using namespace std::chrono;

Communicator::Communicator() {
  vector<string> addrs;
  rpc_poll_ = new PollMgr(1);
  auto config = Config::GetConfig();
  vector<parid_t> partitions = config->GetAllPartitionIds();
  for (auto &par_id : partitions) {
    auto site_infos = config->SitesByPartitionId(par_id);
    vector<std::pair<siteid_t, ClassicProxy*>> proxies;
    for (auto &si : site_infos) {
      auto result = this->ConnectToSite(si, milliseconds(Communicator::CONNECT_TIMEOUT_MS));
      verify(result.first == SUCCESS);
      proxies.push_back(std::make_pair(si.id, result.second));
    }
    rpc_par_proxies_.insert(std::make_pair(par_id, proxies));
  }
}

Communicator::~Communicator() {
  for (auto &pair : rpc_clients_) {
    rrr::Client *rpc_cli = pair.second;
//    rpc_cli->close_and_release();
  }
}

std::pair<siteid_t, ClassicProxy*> Communicator::RandomProxyForPartition(
    parid_t partition_id) const {
  auto it = rpc_par_proxies_.find(partition_id);
  verify(it != rpc_par_proxies_.end());
  auto& partition_proxies = it->second;
  int index = rrr::RandomGenerator::rand(0, partition_proxies.size()-1);
  return partition_proxies[index];
}

std::pair<int, ClassicProxy*> Communicator::ConnectToSite(Config::SiteInfo &site,
                                                          milliseconds timeout) {
  string addr = site.GetHostAddr();
  auto start = steady_clock::now();
  rrr::Client* rpc_cli = new rrr::Client(rpc_poll_);
  double elapsed;
  int attempt = 0;
  do {
    Log::debug("connect to site: %s (attempt %d)", addr.c_str(), attempt++);
    auto connect_result = rpc_cli->connect(addr.c_str());
    if (connect_result == SUCCESS) {
      ClassicProxy *rpc_proxy = new ClassicProxy(rpc_cli);
      rpc_clients_.insert(std::make_pair(site.id, rpc_cli));
      rpc_proxies_.insert(std::make_pair(site.id, rpc_proxy));
      Log::info("connect to site: %s success!", addr.c_str());
      return std::make_pair(SUCCESS, rpc_proxy);
    } else {
      std::this_thread::sleep_for(milliseconds(CONNECT_TIMEOUT_MS / 20));
    }
    auto end = steady_clock::now();
    elapsed = duration_cast<milliseconds>(end - start).count();
  } while(elapsed < timeout.count());
  Log_info("timeout connecting to %s", addr.c_str());
  rpc_cli->close_and_release();
  return std::make_pair(FAILURE, nullptr);
}

} // namespace rococo