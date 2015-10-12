#include "all.h"
#include "client_worker.h"

using namespace rococo;

static ClientControlServiceImpl *ccsi_g = nullptr;

void client_setup_heartbeat() {
  std::map<int32_t, std::string> txn_types;
  TxnRequestFactory::get_txn_types(txn_types);
  unsigned int num_threads = Config::GetConfig()->get_num_threads(); // func
  rrr::Server *hb_server = nullptr;
  bool hb = Config::GetConfig()->do_heart_beat();
  if (hb) {
    // setup controller rpc server
    ccsi_g = new ClientControlServiceImpl(num_threads, txn_types);
    int n_io_threads = 1;
    rrr::PollMgr *poll_mgr = new rrr::PollMgr(n_io_threads);
    base::ThreadPool *thread_pool = new base::ThreadPool(1);
    hb_server = new rrr::Server(poll_mgr, thread_pool);
    hb_server->reg(ccsi_g);
    hb_server->start(std::string("0.0.0.0:").append(
        std::to_string(Config::GetConfig()->get_ctrl_port())).c_str());
  }
}

void client_setup_request_factory() {
  TxnRequestFactory::init_txn_req(nullptr, 0);
}

void client_launch_workers() {
  uint32_t duration = Config::GetConfig()->get_duration();
  if (duration == 0)
    verify(0);

  std::vector<std::string> servers;
  uint32_t num_site = Config::GetConfig()->get_all_site_addr(servers);
  if (num_site == 0)
    verify(0);

  uint32_t num_threads = Config::GetConfig()->get_num_threads();
  std::vector<ClientWorker> worker_attrs(num_threads);
  std::vector<std::thread> client_threads;

  uint32_t start_coo_id = Config::GetConfig()->get_start_coordinator_id(); // func
  int benchmark = Config::GetConfig()->get_benchmark();
  int mode = Config::GetConfig()->get_mode();
  uint32_t concurrent_txn = Config::GetConfig()->get_concurrent_txn();
  bool batch_start = Config::GetConfig()->get_batch_start();
  uint32_t thread_index = 0;
  for (; thread_index < num_threads; thread_index++) {
    worker_attrs[thread_index].servers = &servers;
    worker_attrs[thread_index].coo_id = start_coo_id + thread_index;
    worker_attrs[thread_index].benchmark = benchmark;
    worker_attrs[thread_index].mode = mode;
    worker_attrs[thread_index].batch_start = batch_start;
    worker_attrs[thread_index].id = thread_index;
    worker_attrs[thread_index].duration = duration;
    worker_attrs[thread_index].ccsi = ccsi_g;
    worker_attrs[thread_index].concurrent_txn = concurrent_txn;
    client_threads.push_back(std::thread(&ClientWorker::work,
                                         &worker_attrs[thread_index]));
  }
  for (auto &th: client_threads) {
    th.join();
  }

  TxnRequestFactory::destroy();
  RandomGenerator::destroy();
  Config::DestroyConfig();
}

int main(int argc, char *argv[]) {
  int ret;

  if (0 != (ret = Config::CreateConfig(argc, argv))) {
    Log_fatal("Read config failed");
    return ret;
  }

  //unsigned int cid = Config::get_config()->get_client_id();

  client_setup_request_factory();
  client_setup_heartbeat();
  client_launch_workers();

  return 0;
}
