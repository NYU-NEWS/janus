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
  uint32_t duration = Config::GetConfig()->get_duration();
  if (duration == 0)
    verify(0);

  std::vector<std::string> servers;
  Config::GetConfig()->get_all_site_addr(servers);
  // load some common configuration
  int benchmark = Config::GetConfig()->get_benchmark();
  int mode = Config::GetConfig()->get_mode();
  uint32_t concurrent_txn = Config::GetConfig()->get_concurrent_txn();
  bool batch_start = Config::GetConfig()->get_batch_start();
  // start client workers in new threads.
  vector<Config::SiteInfo*> infos = Config::GetConfig()->GetMyClients();
  vector<std::thread> client_threads;
  vector<ClientWorker> workers(infos.size());
  for (uint32_t thread_index = 0; thread_index < infos.size(); thread_index++) {
    auto &worker = workers[thread_index];
    worker.txn_req_factory_ = new TxnRequestFactory(Config::GetConfig()->sharding_);
    workers[thread_index].servers = &servers;
    workers[thread_index].coo_id = infos[thread_index]->id;
    workers[thread_index].benchmark = benchmark;
    workers[thread_index].mode = mode;
    workers[thread_index].batch_start = batch_start;
    workers[thread_index].id = thread_index;
    workers[thread_index].duration = duration;
    workers[thread_index].ccsi = ccsi_g;
    workers[thread_index].n_outstanding_ = concurrent_txn;
    client_threads.push_back(std::thread(&ClientWorker::work,
                                         &workers[thread_index]));
  }
  for (auto &th: client_threads) {
    th.join();
  }
}


void server_launch_worker() {
  vector<Config::SiteInfo*> infos = Config::GetConfig()->GetMyServers();
  svr_workers = new vector<ServerWorker>(infos.size());

  for (uint32_t index = 0; index < infos.size(); index++) {
    Log_info("launching server, site: %x", infos[index]->id);
    auto &worker = (*svr_workers)[index];
    worker.sharding_ = Frame().CreateSharding(Config::GetConfig()->sharding_);
    worker.sharding_->BuildTableInfoPtr();
    // register txn piece logic
    worker.RegPiece();
    worker.site_info_ = infos[index];
    // setup communication between controller script
    worker.SetupHeartbeat();
    // populate table according to benchmarks
    worker.PopTable();
    // start server service
    worker.SetupService();
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

  vector<Config::SiteInfo*> infos = Config::GetConfig()->GetMyServers();
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
