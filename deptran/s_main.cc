#include "all.h"
#ifdef CPU_PROFILE
# include <google/profiler.h>
#endif // ifdef CPU_PROFILE

#include "frame.h"
#include "client_worker.h"

using namespace rococo;

#include "server_worker.h"

static ClientControlServiceImpl *ccsi_g = nullptr;
static rrr::PollMgr *cli_poll_mgr_g = nullptr;
static rrr::Server *cli_hb_server_g = nullptr;

static vector<ServerWorker>*svr_workers = nullptr;

void client_setup_heartbeat() {
  std::map<int32_t, std::string> txn_types;
  Frame().GetTxnTypes(txn_types);
  unsigned int num_threads = Config::GetConfig()->get_num_threads(); // func
  bool hb = Config::GetConfig()->do_heart_beat();
  if (hb) {
    // setup controller rpc server
    ccsi_g = new ClientControlServiceImpl(num_threads, txn_types);
    int n_io_threads = 1;
    cli_poll_mgr_g = new rrr::PollMgr(n_io_threads);
    base::ThreadPool *thread_pool = new base::ThreadPool(1);
    cli_hb_server_g = new rrr::Server(cli_poll_mgr_g, thread_pool);
    cli_hb_server_g->reg(ccsi_g);
    cli_hb_server_g->start(std::string("0.0.0.0:").append(
        std::to_string(Config::GetConfig()->get_ctrl_port())).c_str());
  }
}

void client_launch_workers() {
  // load some common configuration
  // start client workers in new threads.
  vector<Config::SiteInfo> client_sites = Config::GetConfig()->GetMyClients();
  vector<std::thread> client_threads;
  vector<ClientWorker*> workers;
  for (uint32_t client_id = 0; client_id < client_sites.size(); client_id++) {
    ClientWorker* worker = new ClientWorker(client_id, client_sites[client_id],
                                   Config::GetConfig(), ccsi_g);
    workers.push_back(worker);
    client_threads.push_back(std::thread(&ClientWorker::work,
                                         worker));
  }
  for (auto &th: client_threads) {
    th.join();
  }
  for (auto worker : workers) {
    delete worker;
  }
}


void server_launch_worker() {
  Config* cfg = Config::GetConfig();
  for (auto& replica_group : cfg->replica_groups_) {
    for (auto& site_info : replica_group.replicas) {
        Log_info("launching site: %x, bind address %s", site_info.id, site_info.GetBindAddress().c_str());
        auto worker = new ServerWorker();
        worker->sharding_ = Frame().CreateSharding(Config::GetConfig()->sharding_);
        worker->sharding_->BuildTableInfoPtr();
        // register txn piece logic
        worker->RegPiece();
        worker->site_info_ = &site_info;
        // setup communication between controller script
        worker->SetupHeartbeat();
        // populate table according to benchmarks
        worker->PopTable();
        // start server service
        worker->SetupService();
    }
  }
}

void server_shutdown() {
  // TODO
  for (auto &worker : *svr_workers) {
    worker.ShutDown();
  }
}

void check_current_path() {
  auto path = boost::filesystem::current_path();
  Log_info("PWD : ", path.string().c_str());
}

int main(int argc, char *argv[]) {
  int ret;
  check_current_path();

  // read configuration
  if (0 != (ret = Config::CreateConfig(argc, argv))) {
    Log_fatal("Read config failed");
    return ret;
  }

  vector<Config::SiteInfo> infos = Config::GetConfig()->GetMyServers();
  if (infos.size() > 0) {
    Log_info("server enabled, number of sites: %d", infos.size());

    // start server service
    server_launch_worker();
  }

  infos = Config::GetConfig()->GetMyClients();
  if (infos.size() > 0) {
    Log_info("client enabled, number of sites: %d", infos.size());
    client_setup_heartbeat();
    client_launch_workers();
    // TODO shutdown servers.
    server_shutdown();
  } else {
    Log_info("No clients running in this process, just sleep and wait.");
    while (1) {
      sleep(1000);
    }
  }

  RandomGenerator::destroy();
  Config::DestroyConfig();

  return 0;
}
