#include "paxos_worker.h"
#include "client_worker.h"
#include "procedure.h"
#include "command_marshaler.h"
#include "benchmark_control_rpc.h"
#include "server_worker.h"

#ifdef CPU_PROFILE
# include <gperftools/profiler.h>
#endif // ifdef CPU_PROFILE
#include "config.h"
#include <sys/time.h>

#ifdef CPU_PROFILE
#include <gperftools/profiler.h>
#endif // ifdef CPU_PROFILE
using namespace janus;

static vector<unique_ptr<PaxosWorker>> pxs_workers_g = {};
// vector<unique_ptr<ClientWorker>> client_workers_g = {};

void check_current_path() {
  auto path = boost::filesystem::current_path();
  Log_info("PWD : %s", path.string().c_str());
}

void server_launch_worker(vector<Config::SiteInfo>& server_sites) {
  auto config = Config::GetConfig();
  Log_info("server enabled, number of sites: %d", server_sites.size());
  for (int i = server_sites.size(); i; --i) {
    PaxosWorker* worker = new PaxosWorker();
    pxs_workers_g.push_back(std::unique_ptr<PaxosWorker>(worker));
  }

  int i = 0;
  vector<std::thread> setup_ths;
  for (auto& site_info : server_sites) {
    setup_ths.push_back(std::thread([&site_info, &i, &config]() {
      Log_info("launching site: %x, bind address %s",
               site_info.id,
               site_info.GetBindAddress().c_str());
      auto& worker = pxs_workers_g[i++];
      worker->site_info_ = const_cast<Config::SiteInfo*>(&config->SiteById(site_info.id));

      // setup frame and scheduler
      worker->SetupBase();
      // start server service
      worker->SetupService();
      // setup communicator
      worker->SetupCommo();
      // register callback
      worker->register_apply_callback([=](char* log, int len) {
        // Log_info("!!!!!!!!!!!!!!!!!!!!%s!!!!!!!!!!!!!!!!", log);
      });
      Log_info("site %d launched!", (int)site_info.id);
    }));
  }
  Log_info("waiting for server setup threads.");
  for (auto& th : setup_ths) {
    th.join();
  }
  Log_info("done waiting for server setup threads.");

  for (auto& worker : pxs_workers_g) {
    // start communicator after all servers are running
    // setup communication between controller script
    worker->SetupHeartbeat();
  }
  Log_info("server workers' communicators setup");
}

const int len = 10, num = 500000, concurrent = 32;
char* message[concurrent];
void microbench_paxos() {
  int T = num;
  while (T > 0) {
    struct timeval t1, t2;
    gettimeofday(&t1, NULL);
    for (int i = 0; i < concurrent; i++) {
      message[i] = new char[len];
      for (int j = 0; j < len; j++) {
        message[i][j] = (rand() % 10) + '0';
      }
      for (auto& worker : pxs_workers_g) {
        worker->Submit(message[i], len);
      }
    }
    for (auto& worker : pxs_workers_g) {
      worker->WaitForSubmit();
    }
    gettimeofday(&t2, NULL);
    pxs_workers_g[0]->submit_tot_sec_ += t2.tv_sec - t1.tv_sec;
    pxs_workers_g[0]->submit_tot_usec_ += t2.tv_usec - t1.tv_usec;
    T -= concurrent;
    // if ((num - T) % (concurrent * 10) == 0) Log_info("%d%% finished", (num - T) * 100 / num);
  }
}

int main(int argc, char* argv[]) {
  check_current_path();
  Log_info("starting process %ld", getpid());

  int ret = Config::CreateConfig(argc, argv);
  if (ret != SUCCESS) {
    Log_fatal("Read config failed");
    return ret;
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
  microbench_paxos();

  for (auto& worker : pxs_workers_g) {
    worker->WaitForShutdown();
  }
#ifdef CPU_PROFILE
  // stop profiling
  ProfilerStop();
#endif // ifdef CPU_PROFILE
  Log_info("all server workers have shut down.");

  fflush(stderr);
  fflush(stdout);

  for (auto& worker : pxs_workers_g) {
    worker->ShutDown();
  }
  pxs_workers_g.clear();

  for (int i = 0; i < concurrent; i++) {
    delete message[i];
  }
  RandomGenerator::destroy();
  Config::DestroyConfig();

  Log_debug("exit process.");

  return 0;
}
