
#include "communicator.h"
#include "coordinator.h"
#include "rococo/graph.h"
#include "rococo/graph_marshaler.h"
#include "command.h"
#include "command_marshaler.h"
#include "procedure.h"
#include "rcc_rpc.h"

namespace janus {

Communicator::Communicator(PollMgr* poll_mgr) {
  vector<string> addrs;
  if (poll_mgr == nullptr)
    rpc_poll_ = new PollMgr(1);
  else
    rpc_poll_ = poll_mgr;
  auto config = Config::GetConfig();
  vector<parid_t> partitions = config->GetAllPartitionIds();
  for (auto& par_id : partitions) {
    auto site_infos = config->SitesByPartitionId(par_id);
    vector<std::pair<siteid_t, ClassicProxy*>> proxies;
    for (auto& si : site_infos) {
      auto result = ConnectToSite(si, std::chrono::milliseconds
          (CONNECT_TIMEOUT_MS));
      verify(result.first == SUCCESS);
      proxies.push_back(std::make_pair(si.id, result.second));
    }
    rpc_par_proxies_.insert(std::make_pair(par_id, proxies));
  }
  client_leaders_connected_.store(false);
  if (config->forwarding_enabled_) {
    threads.push_back(std::thread(&Communicator::ConnectClientLeaders, this));
  } else {
    client_leaders_connected_.store(true);
  }
}

void Communicator::ConnectClientLeaders() {
  auto config = Config::GetConfig();
  if (config->forwarding_enabled_) {
    Log_info("%s: connect to client sites", __FUNCTION__);
    auto client_leaders = config->SitesByLocaleId(0, Config::CLIENT);
    for (Config::SiteInfo leader_site_info : client_leaders) {
      verify(leader_site_info.locale_id == 0);
      Log_info("client @ leader %d", leader_site_info.id);
      auto result = ConnectToClientSite(leader_site_info,
                                        std::chrono::milliseconds
                                            (CONNECT_TIMEOUT_MS));
      verify(result.first == SUCCESS);
      verify(result.second != nullptr);
      Log_info("connected to client leader site: %d, %d, %p",
               leader_site_info.id,
               leader_site_info.locale_id,
               result.second);
      client_leaders_.push_back(std::make_pair(leader_site_info.id,
                                               result.second));
    }
  }
  client_leaders_connected_.store(true);
}

void Communicator::WaitConnectClientLeaders() {
  bool connected;
  do {
    connected = client_leaders_connected_.load();
  } while (!connected);
  Log_info("Done waiting to connect to client leaders.");
}

Communicator::~Communicator() {
  verify(rpc_clients_.size() > 0);
  for (auto& pair : rpc_clients_) {
    rrr::Client* rpc_cli = pair.second;
    rpc_cli->close_and_release();
  }
  rpc_clients_.clear();
}

std::pair<siteid_t, ClassicProxy*>
Communicator::RandomProxyForPartition(parid_t par_id) const {
  auto it = rpc_par_proxies_.find(par_id);
  verify(it != rpc_par_proxies_.end());
  auto& par_proxies = it->second;
  int index = rrr::RandomGenerator::rand(0, par_proxies.size() - 1);
  return par_proxies[index];
}

std::pair<siteid_t, ClassicProxy*>
Communicator::LeaderProxyForPartition(parid_t par_id) const {
  auto leader_cache =
      const_cast<map<parid_t, SiteProxyPair>&>(this->leader_cache_);
  auto leader_it = leader_cache.find(par_id);
  if (leader_it != leader_cache.end()) {
    return leader_it->second;
  } else {
    auto it = rpc_par_proxies_.find(par_id);
    verify(it != rpc_par_proxies_.end());
    auto& partition_proxies = it->second;
    auto config = Config::GetConfig();
    auto proxy_it = std::find_if(
        partition_proxies.begin(),
        partition_proxies.end(),
        [config](const std::pair<siteid_t, ClassicProxy*>& p) {
          verify(p.second != nullptr);
          auto& site = config->SiteById(p.first);
          return site.locale_id == 0;
        });
    if (proxy_it == partition_proxies.end()) {
      Log_fatal("could not find leader for partition %d", par_id);
    } else {
      leader_cache[par_id] = *proxy_it;
      Log_debug("leader site for parition %d is %d", par_id, proxy_it->first);
    }
    verify(proxy_it->second != nullptr);
    return *proxy_it;
  }
}

ClientSiteProxyPair
Communicator::ConnectToClientSite(Config::SiteInfo& site,
                                  std::chrono::milliseconds timeout) {
  auto config = Config::GetConfig();
  char addr[1024];
  snprintf(addr, sizeof(addr), "%s:%d", site.host.c_str(), site.port);

  auto start = std::chrono::steady_clock::now();
  rrr::Client* rpc_cli = new rrr::Client(rpc_poll_);
  double elapsed;
  int attempt = 0;
  do {
    Log_info("connect to client site: %s (attempt %d)", addr, attempt++);
    auto connect_result = rpc_cli->connect(addr);
    if (connect_result == SUCCESS) {
      ClientControlProxy* rpc_proxy = new ClientControlProxy(rpc_cli);
      rpc_clients_.insert(std::make_pair(site.id, rpc_cli));
      Log_debug("connect to client site: %s success!", addr);
      return std::make_pair(SUCCESS, rpc_proxy);
    } else {
      std::this_thread::sleep_for(std::chrono::milliseconds(CONNECT_SLEEP_MS));
    }
    auto end = std::chrono::steady_clock::now();
    elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
        .count();
  } while (elapsed < timeout.count());
  Log_info("timeout connecting to client %s", addr);
  rpc_cli->close_and_release();
  return std::make_pair(FAILURE, nullptr);
}

std::pair<int, ClassicProxy*>
Communicator::ConnectToSite(Config::SiteInfo& site,
                            std::chrono::milliseconds timeout) {
  string addr = site.GetHostAddr();
  auto start = std::chrono::steady_clock::now();
  rrr::Client* rpc_cli = new rrr::Client(rpc_poll_);
  double elapsed;
  int attempt = 0;
  do {
    Log_info("connect to site: %s (attempt %d)", addr.c_str(), attempt++);
    auto connect_result = rpc_cli->connect(addr.c_str());
    if (connect_result == SUCCESS) {
      ClassicProxy* rpc_proxy = new ClassicProxy(rpc_cli);
      rpc_clients_.insert(std::make_pair(site.id, rpc_cli));
      rpc_proxies_.insert(std::make_pair(site.id, rpc_proxy));
      Log_debug("connect to site: %s success!", addr.c_str());
      return std::make_pair(SUCCESS, rpc_proxy);
    } else {
      std::this_thread::sleep_for(std::chrono::milliseconds(CONNECT_SLEEP_MS));
    }
    auto end = std::chrono::steady_clock::now();
    elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
        .count();
  } while (elapsed < timeout.count());
  Log_info("timeout connecting to %s", addr.c_str());
  rpc_cli->close_and_release();
  return std::make_pair(FAILURE, nullptr);
}

std::pair<siteid_t, ClassicProxy*>
Communicator::NearestProxyForPartition(parid_t par_id) const {
  // TODO Fix me.
  auto it = rpc_par_proxies_.find(par_id);
  verify(it != rpc_par_proxies_.end());
  auto& partition_proxies = it->second;
  verify(partition_proxies.size() > loc_id_);
  int index = loc_id_;
  return partition_proxies[index];
};

void Communicator::BroadcastDispatch(
    shared_ptr<vector<shared_ptr<TxPieceData>>> sp_vec_piece,
    Coordinator* coo,
    const function<void(int, TxnOutput&)> & callback) {
  cmdid_t cmd_id = sp_vec_piece->at(0)->root_id_;
  verify(sp_vec_piece->size() > 0);
  auto par_id = sp_vec_piece->at(0)->PartitionId();
  rrr::FutureAttr fuattr;
  fuattr.callback =
      [coo, this, callback](Future* fu) {
        int32_t ret;
        TxnOutput outputs;
        fu->get_reply() >> ret >> outputs;
        callback(ret, outputs);
      };
  auto pair_leader_proxy = LeaderProxyForPartition(par_id);
  Log_debug("send dispatch to site %ld",
            pair_leader_proxy.first);
  auto proxy = pair_leader_proxy.second;
  shared_ptr<VecPieceData> sp_vpd(new VecPieceData);
  sp_vpd->sp_vec_piece_data_ = sp_vec_piece;
  MarshallDeputy md(sp_vpd); // ????
  auto future = proxy->async_Dispatch(cmd_id, md, fuattr);
  Future::safe_release(future);
  // FIXME fix this, this cause occ and perhaps 2pl to fail
  for (auto& pair : rpc_par_proxies_[par_id]) {
    if (pair.first != pair_leader_proxy.first) {
      rrr::FutureAttr fuattr;
      fuattr.callback =
          [coo, this, callback](Future* fu) {
            int32_t ret;
            TxnOutput outputs;
            fu->get_reply() >> ret >> outputs;
            // do nothing
          };
      Future::safe_release(pair.second->async_Dispatch(cmd_id, md, fuattr));
    }
  }
}

void Communicator::SendStart(SimpleCommand& cmd,
                             int32_t output_size,
                             std::function<void(Future* fu)>& callback) {
  verify(0);
}

void Communicator::SendPrepare(groupid_t gid,
                               txnid_t tid,
                               std::vector<int32_t>& sids,
                               const function<void(int)>& callback) {
  FutureAttr fuattr;
  std::function<void(Future*)> cb =
      [this, callback](Future* fu) {
        int res;
        fu->get_reply() >> res;
        callback(res);
      };
  fuattr.callback = cb;
  ClassicProxy* proxy = LeaderProxyForPartition(gid).second;
  Log_debug("SendPrepare to %ld sites gid:%ld, tid:%ld\n",
            sids.size(),
            gid,
            tid);
  Future::safe_release(proxy->async_Prepare(tid, sids, fuattr));
}

void Communicator::___LogSent(parid_t pid, txnid_t tid) {
  auto value = std::make_pair(pid, tid);
  auto it = phase_three_sent_.find(value);
  if (it != phase_three_sent_.end()) {
    Log_fatal("phase 3 sent exists: %d %x", it->first, it->second);
  } else {
    phase_three_sent_.insert(value);
    Log_debug("phase 3 sent: pid: %d; tid: %x", value.first, value.second);
  }
}

void Communicator::SendCommit(parid_t pid,
                              txnid_t tid,
                              const function<void()>& callback) {
#ifdef LOG_LEVEL_AS_DEBUG
  ___LogSent(pid, tid);
#endif
  FutureAttr fuattr;
  fuattr.callback = [callback](Future*) { callback(); };
  ClassicProxy* proxy = LeaderProxyForPartition(pid).second;
  Log_debug("SendCommit to %ld tid:%ld\n", pid, tid);
  Future::safe_release(proxy->async_Commit(tid, fuattr));
}

void Communicator::SendAbort(parid_t pid, txnid_t tid,
                             const function<void()>& callback) {
#ifdef LOG_LEVEL_AS_DEBUG
  ___LogSent(pid, tid);
#endif
  FutureAttr fuattr;
  fuattr.callback = [callback](Future*) { callback(); };
  ClassicProxy* proxy = LeaderProxyForPartition(pid).second;
  Log_debug("SendAbort to %ld tid:%ld\n", pid, tid);
  Future::safe_release(proxy->async_Abort(tid, fuattr));
}

void Communicator::SendUpgradeEpoch(epoch_t curr_epoch,
                                    const function<void(parid_t,
                                                        siteid_t,
                                                        int32_t&)>& callback) {
  for (auto& pair: rpc_par_proxies_) {
    auto& par_id = pair.first;
    auto& proxies = pair.second;
    for (auto& pair: proxies) {
      FutureAttr fuattr;
      auto& site_id = pair.first;
      function<void(Future*)> cb = [callback, par_id, site_id](Future* fu) {
        int32_t res;
        fu->get_reply() >> res;
        callback(par_id, site_id, res);
      };
      fuattr.callback = cb;
      auto proxy = (ClassicProxy*) pair.second;
      Future::safe_release(proxy->async_UpgradeEpoch(curr_epoch, fuattr));
    }
  }
}

void Communicator::SendTruncateEpoch(epoch_t old_epoch) {
  for (auto& pair: rpc_par_proxies_) {
    auto& par_id = pair.first;
    auto& proxies = pair.second;
    for (auto& pair: proxies) {
      FutureAttr fuattr;
      fuattr.callback = [](Future*) {};
      auto proxy = (ClassicProxy*) pair.second;
      Future::safe_release(proxy->async_TruncateEpoch(old_epoch));
    }
  }
}

void Communicator::SendForwardTxnRequest(
    TxRequest& req,
    Coordinator* coo,
    std::function<void(const TxReply&)> callback) {
  Log_info("%s: %d, %d", __FUNCTION__, coo->coo_id_, coo->par_id_);
  verify(client_leaders_.size() > 0);
  auto idx = rrr::RandomGenerator::rand(0, client_leaders_.size() - 1);
  auto p = client_leaders_[idx];
  auto leader_site_id = p.first;
  auto leader_proxy = p.second;
  Log_debug("%s: send to client site %d", __FUNCTION__, leader_site_id);
  TxDispatchRequest dispatch_request;
  dispatch_request.id = coo->coo_id_;
  for (size_t i = 0; i < req.input_.size(); i++) {
    dispatch_request.input.push_back(req.input_[i]);
  }
  dispatch_request.tx_type = req.tx_type_;

  FutureAttr future;
  future.callback = [callback](Future* f) {
    TxReply reply;
    f->get_reply() >> reply;
    callback(reply);
  };
  Future::safe_release(leader_proxy->async_DispatchTxn(dispatch_request,
                                                       future));
}

vector<shared_ptr<MessageEvent>>
Communicator::BroadcastMessage(shardid_t shard_id,
                               svrid_t svr_id,
                               string& msg) {
  verify(0);
  // TODO
  vector<shared_ptr<MessageEvent>> events;

  for (auto& p : rpc_par_proxies_[shard_id]) {
    auto site_id = p.first;
    auto proxy = (p.second);
    verify(proxy != nullptr);
    FutureAttr fuattr;
    auto msg_ev = std::make_shared<MessageEvent>(shard_id, site_id);
    events.push_back(msg_ev);
    fuattr.callback = [msg_ev] (Future* fu) {
      auto& marshal = fu->get_reply();
      marshal >> msg_ev->msg_;
      msg_ev->Set(1);
    };
    Future* f = nullptr;
    Future::safe_release(f);
  }
  return events;
}

shared_ptr<MessageEvent>
Communicator::SendMessage(siteid_t site_id,
                          string& msg) {
  verify(0);
  // TODO
  auto ev = std::make_shared<MessageEvent>(site_id);
  return ev;
}

void Communicator::AddMessageHandler(
    function<bool(const string&, string&)> f) {
  msg_string_handlers_.push_back(f);
}

void Communicator::AddMessageHandler(
    function<bool(const MarshallDeputy&, MarshallDeputy&)> f) {
  msg_marshall_handlers_.push_back(f);
}
} // namespace janus
