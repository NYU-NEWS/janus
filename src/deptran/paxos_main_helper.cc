
#include "__dep__.h"
#include "frame.h"
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
#include "s_main.h"

using namespace janus;

vector<unique_ptr<ClientWorker>> client_workers_g = {};
static vector<unique_ptr<PaxosWorker>> pxs_workers_g = {};
// vector<unique_ptr<ClientWorker>> client_workers_g = {};
const int len = 5;

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

char* message[200];
void microbench_paxos() {
  // register callback
  for (auto& worker : pxs_workers_g) {
    if (worker->IsLeader(worker->site_info_->partition_id_))
      worker->register_apply_callback([&worker](const char* log, int len) {
        Log_debug("submit callback enter in");
        if (worker->submit_num >= worker->tot_num) return;
        worker->Submit(log, len, worker->site_info_->partition_id_);
        worker->submit_num++;
      });
    else
      worker->register_apply_callback([=](const char* log, int len) {});
  }
  auto client_infos = Config::GetConfig()->GetMyClients();
  if (client_infos.size() <= 0) return;
  int concurrent = Config::GetConfig()->get_concurrent_txn();
  for (int i = 0; i < concurrent; i++) {
    message[i] = new char[len];
    message[i][0] = (i / 100) + '0';
    message[i][1] = ((i / 10) % 10) + '0';
    message[i][2] = (i % 10) + '0';
    for (int j = 3; j < len - 1; j++) {
      message[i][j] = (rand() % 10) + '0';
    }
    message[i][len - 1] = '\0';
  }
#ifdef CPU_PROFILE
  char prof_file[1024];
  Config::GetConfig()->GetProfilePath(prof_file);
  // start to profile
  ProfilerStart(prof_file);
#endif // ifdef CPU_PROFILE
  struct timeval t1, t2;
  gettimeofday(&t1, NULL);
  for (int i = 0; i < concurrent; i++) {
    for (auto& worker : pxs_workers_g) {
      worker->Submit(message[i], len, worker->site_info_->partition_id_);
    }
  }
  for (auto& worker : pxs_workers_g) {
    worker->WaitForSubmit();
  }
  gettimeofday(&t2, NULL);
  pxs_workers_g[0]->submit_tot_sec_ += t2.tv_sec - t1.tv_sec;
  pxs_workers_g[0]->submit_tot_usec_ += t2.tv_usec - t1.tv_usec;
#ifdef CPU_PROFILE
  // stop profiling
  ProfilerStop();
#endif // ifdef CPU_PROFILE

  for (int i = 0; i < concurrent; i++) {
    delete message[i];
  }
  Log_info("shutdown Server Control Service after task finish");
  for (auto& worker : pxs_workers_g) {
    if (worker->hb_rpc_server_ != nullptr) {
      worker->scsi_->server_shutdown(nullptr);
    }
  }
}

int setup(int argc, char* argv[]) {
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
  return 0;
}

int shutdown_paxos() {
  for (auto& worker : pxs_workers_g) {
    worker->WaitForShutdown();
  }
  Log_info("all server workers have shut down.");

  fflush(stderr);
  fflush(stdout);

  for (auto& worker : pxs_workers_g) {
    worker->ShutDown();
  }
  pxs_workers_g.clear();

  RandomGenerator::destroy();
  Config::DestroyConfig();

  Log_info("exit process.");

  return 0;
}

void register_for_follower(std::function<void(const char*, int)> cb, uint32_t par_id) {
  for (auto& worker : pxs_workers_g) {
    if (worker->IsPartition(par_id) && !worker->IsLeader(par_id)) {
      worker->register_apply_callback(cb);
    }
  }
}

void register_for_leader(std::function<void(const char*, int)> cb, uint32_t par_id) {
  for (auto& worker : pxs_workers_g) {
    if (worker->IsLeader(par_id)) {
      worker->register_apply_callback(cb);
    }
  }
}

void submit(const char* log, int len, uint32_t par_id) {
  for (auto& worker : pxs_workers_g) {
    // worker->Submit(log, len);
    if (!worker->IsLeader(par_id)) continue;
    verify(worker->submit_pool != nullptr);
    if (worker->submit_pool->add(
            [=, &worker]() {
              worker->Submit(log, len, par_id);
            }) != 0) {
      Log_fatal("paxos submit_pool error!");
    }
  }
}

void wait_for_submit(uint32_t par_id) {
  for (auto& worker : pxs_workers_g) {
    if (!worker->IsLeader(par_id)) continue;
    verify(worker->submit_pool != nullptr);
    worker->submit_pool->wait_for_all();
    worker->WaitForSubmit();
  }
}

void microbench_paxos_queue() {
  // register callback
  for (auto& worker : pxs_workers_g) {
    if (worker->IsLeader(worker->site_info_->partition_id_))
      worker->register_apply_callback([&worker](const char* log, int len) {
        Log_debug("submit callback enter in");
        if (worker->submit_num >= worker->tot_num) return;
        worker->submit_num++;
        submit(log, len, worker->site_info_->partition_id_);
      });
    else
      worker->register_apply_callback([&worker](const char* log, int len) {
        std::ofstream outfile(string("log/") + string(worker->site_info_->name.c_str()) + string(".txt"), ios::app);
	outfile << log << std::endl;
	outfile.close();
      });
  }
  auto client_infos = Config::GetConfig()->GetMyClients();
  if (client_infos.size() <= 0) return;
  int concurrent = Config::GetConfig()->get_concurrent_txn();
  for (int i = 0; i < concurrent; i++) {
    message[i] = new char[len];
    message[i][0] = (i / 100) + '0';
    message[i][1] = ((i / 10) % 10) + '0';
    message[i][2] = (i % 10) + '0';
    for (int j = 3; j < len - 1; j++) {
      message[i][j] = (rand() % 10) + '0';
    }
    message[i][len - 1] = '\0';
  }
#ifdef CPU_PROFILE
  char prof_file[1024];
  Config::GetConfig()->GetProfilePath(prof_file);
  // start to profile
  ProfilerStart(prof_file);
#endif // ifdef CPU_PROFILE
  struct timeval t1, t2;
  gettimeofday(&t1, NULL);
  vector<std::thread> ths;
  int k = 0;
  for (int j = 0; j < 2; j++) {
    ths.push_back(std::thread([=, &k]() {
      int par_id = k++;
      for (int i = 0; i < concurrent; i++) {
        submit(message[i], len, par_id);
        // wait_for_submit(j);
      }
    }));
  }
  Log_info("waiting for submission threads.");
  for (auto& th : ths) {
    th.join();
  }
  while (1) {
    for (int j = 0; j < 2; j++) {
      wait_for_submit(j);
    }
    bool flag = true;
    for (auto& worker : pxs_workers_g) {
      if (worker->tot_num > worker->submit_num)
        flag = false;
    }
    if (flag) {
      Log_info("microbench finishes");
      break;
    }
  }
  gettimeofday(&t2, NULL);
  pxs_workers_g[0]->submit_tot_sec_ += t2.tv_sec - t1.tv_sec;
  pxs_workers_g[0]->submit_tot_usec_ += t2.tv_usec - t1.tv_usec;
#ifdef CPU_PROFILE
  // stop profiling
  ProfilerStop();
#endif // ifdef CPU_PROFILE

  Log_info("%s, time consumed: %f", pxs_workers_g[0]->site_info_->name.c_str(),
           pxs_workers_g[0]->submit_tot_sec_ + ((float)pxs_workers_g[0]->submit_tot_usec_) / 1000000);
  for (int i = 0; i < concurrent; i++) {
    delete message[i];
  }
  Log_info("shutdown Server Control Service after task finish");
  for (auto& worker : pxs_workers_g) {
    if (worker->hb_rpc_server_ != nullptr) {
      worker->scsi_->server_shutdown(nullptr);
    }
  }
}
