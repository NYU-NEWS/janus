
#include <unistd.h>
#include "__dep__.h"
#include "frame.h"
#include "client_worker.h"
#include "txn_chopper.h"
#include "command_marshaler.h"
#include "benchmark_control_rpc.h"
#include "server_worker.h"

#ifdef CPU_PROFILE
# include <google/profiler.h>
#endif // ifdef CPU_PROFILE

using namespace rococo;

static ClientControlServiceImpl *ccsi_g = nullptr;
static rrr::PollMgr *cli_poll_mgr_g = nullptr;
static rrr::Server *cli_hb_server_g = nullptr;

static vector<ServerWorker> svr_workers_g = {};
vector<ClientWorker*> client_workers_g = {};
static std::vector<std::thread> client_threads_g = {};

void client_setup_heartbeat(int num_clients) {
  Log_info("%s", __FUNCTION__);
  std::map<int32_t, std::string> txn_types;
  Frame* f = Frame::GetFrame(Config::GetConfig()->cc_mode_);
  f->GetTxnTypes(txn_types);
  delete f;
  bool hb = Config::GetConfig()->do_heart_beat();
  if (hb) {
    // setup controller rpc server
    ccsi_g = new ClientControlServiceImpl(num_clients, txn_types);
    int n_io_threads = 1;
    cli_poll_mgr_g = new rrr::PollMgr(n_io_threads);
    base::ThreadPool *thread_pool = new base::ThreadPool(1);
    cli_hb_server_g = new rrr::Server(cli_poll_mgr_g, thread_pool);
    cli_hb_server_g->reg(ccsi_g);
    auto ctrl_port = std::to_string(Config::GetConfig()->get_ctrl_port());
    std::string server_address = std::string("0.0.0.0:").append(ctrl_port);
    Log_info("Start control server on port %s", ctrl_port.c_str());
    cli_hb_server_g->start(server_address.c_str());
  }
}


void client_launch_workers(vector<Config::SiteInfo> &client_sites) {
  // load some common configuration
  // start client workers in new threads.
  Log_info("client enabled, number of sites: %d", client_sites.size());
  vector<ClientWorker*> workers;

  for (uint32_t client_id = 0; client_id < client_sites.size(); client_id++) {
    ClientWorker* worker = new ClientWorker(client_id,
                                            client_sites[client_id],
                                            Config::GetConfig(),
                                            ccsi_g);
    workers.push_back(worker);
    client_threads_g.push_back(std::thread(&ClientWorker::work,
                                         worker));
    client_workers_g.push_back(worker);
  }

}

void server_launch_worker(vector<Config::SiteInfo>& server_sites) {
  auto config = Config::GetConfig();
  Log_info("server enabled, number of sites: %d", server_sites.size());
  svr_workers_g.resize(server_sites.size(), ServerWorker());
  int i=0;
  vector<std::thread> setup_ths;
  for (auto& site_info : server_sites) {
    setup_ths.push_back(std::thread([&site_info, &i, &config] () {
      Log_info("launching site: %x, bind address %s",
               site_info.id,
               site_info.GetBindAddress().c_str());
      auto& worker = svr_workers_g[i++];
      worker.site_info_ = const_cast<Config::SiteInfo*>(&config->SiteById(site_info.id));
      worker.SetupBase();
      // register txn piece logic
      worker.RegPiece();
      // populate table according to benchmarks
      worker.PopTable();
      Log_info("table popped for site %d", (int)worker.site_info_->id);
      // start server service
      worker.SetupService();
      Log_info("start communication for site %d", (int)worker.site_info_->id);
      worker.SetupCommo();
      Log_info("site %d launched!", (int)site_info.id);
    }));
  }

  Log_info("waiting for client setup threads.");
  for (auto& th: setup_ths) {
    th.join();
  }
  Log_info("done waiting for client setup threads.");

  for (ServerWorker& worker : svr_workers_g) {
    // start communicator after all servers are running
    // setup communication between controller script
    worker.SetupHeartbeat();
  }
  Log_info("server workers' communicators setup");
}

void client_shutdown() {
  for (auto& worker : client_workers_g) {
    delete worker;
  }
}

void server_shutdown() {
  for (auto &worker : svr_workers_g) {
    worker.ShutDown();
  }
}

void check_current_path() {
  auto path = boost::filesystem::current_path();
  Log_info("PWD : ", path.string().c_str());
}

void wait_for_clients() {
  Log_info("%s: wait for client threads to exit.", __FUNCTION__);
  for (auto &th: client_threads_g) {
    th.join();
  }
}

int main(int argc, char *argv[]) {
  check_current_path();
  Log_info("starting process %ld", getpid());

  // read configuration
  int ret = Config::CreateConfig(argc, argv);
  if (ret != SUCCESS) {
    Log_fatal("Read config failed");
    return ret;
  }

  auto client_infos = Config::GetConfig()->GetMyClients();
  if (client_infos.size() > 0) {
    client_setup_heartbeat(client_infos.size());
  }


  auto server_infos = Config::GetConfig()->GetMyServers();
  if (server_infos.size() > 0) {
    server_launch_worker(server_infos);
  }

#ifdef CPU_PROFILE
  char prof_file[1024];
  Config::GetConfig()->GetProfilePath(prof_file);
  // start to profile
  ProfilerStart(prof_file);
#endif // ifdef CPU_PROFILE
  if (client_infos.size() > 0) {
    //client_setup_heartbeat(client_infos.size());
    client_launch_workers(client_infos);
    wait_for_clients();
    Log_info("all clients have shut down.");
  }

  for (auto& worker : svr_workers_g) {
    worker.WaitForShutdown();
  }
#ifdef CPU_PROFILE
  // stop profiling
  ProfilerStop();
#endif // ifdef CPU_PROFILE
  Log_info("all server workers have shut down.");

  // TODO, FIXME pending_future in rpc cause error.
  fflush(stderr);
  fflush(stdout);
  return 0;
  client_shutdown();
  server_shutdown();

  RandomGenerator::destroy();
  Config::DestroyConfig();

  Log_debug("exit process.");

  return 0;
}
