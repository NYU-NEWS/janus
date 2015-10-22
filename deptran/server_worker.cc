
#include "server_worker.h"
#include "rcc_service.h"
#include "benchmark_control_rpc.h"
#include "sharding.h"


namespace rococo {

//rrr::PollMgr* ServerWorker::svr_poll_mgr_g = nullptr;
//RococoServiceImpl* ServerWorker::rsi_g = nullptr;
//rrr::Server* ServerWorker::svr_server_g = nullptr;
//base::ThreadPool* ServerWorker::thread_pool_g = nullptr;
//
//rrr::PollMgr* ServerWorker::svr_hb_poll_mgr_g = NULL;
//ServerControlServiceImpl* ServerWorker::scsi_g = NULL;
//rrr::Server* ServerWorker::svr_hb_server_g = NULL;
//base::ThreadPool* ServerWorker::hb_thread_pool_g = NULL;

void ServerWorker::SetupHeartbeat() {
  bool hb = Config::GetConfig()->do_heart_beat();
  if (!hb) return;
  auto timeout = Config::GetConfig()->get_ctrl_timeout();
  scsi_g = new ServerControlServiceImpl(timeout);
  int n_io_threads = 1;
  svr_hb_poll_mgr_g = new rrr::PollMgr(n_io_threads);
  hb_thread_pool_g = new rrr::ThreadPool(1);
  svr_hb_server_g = new rrr::Server(svr_hb_poll_mgr_g, hb_thread_pool_g);
  svr_hb_server_g->reg(scsi_g);
  auto port = Config::GetConfig()->get_ctrl_port();
  std::string addr_port = std::string("0.0.0.0:") +
      std::to_string(port);
  svr_hb_server_g->start(addr_port.c_str());
}

void ServerWorker::PopTable() {

  // TODO this needs to be done before poping table
  int running_mode = Config::GetConfig()->get_mode();

  // populate table
  txn_mgr_ = DTxnMgr::CreateTxnMgr(site_info_->id);
  auto par_id = site_info_->par_id;
  int ret = 0;

  // get all tables
  std::vector<std::string> table_names;

  ret = Sharding::get_table_names(site_info_->id, table_names);
  verify(ret > 0);

  std::vector<std::string>::iterator table_it = table_names.begin();

  for (; table_it != table_names.end(); table_it++) {
    mdb::Schema *schema = new mdb::Schema();
    mdb::symbol_t symbol;
    Sharding::init_schema(*table_it, schema, &symbol);
    mdb::Table *tb;
    switch (symbol) {
      case mdb::TBL_SORTED:
        tb = new mdb::SortedTable(schema);
        break;
      case mdb::TBL_UNSORTED:
        tb = new mdb::UnsortedTable(schema);
        break;
      case mdb::TBL_SNAPSHOT:
        tb = new mdb::SnapshotTable(schema);
        break;
      default:
        verify(0);
    }
    txn_mgr_->reg_table(*table_it, tb);
  }
  verify(Sharding::sharding_s);
  Sharding::sharding_s->PopulateTable(site_info_->id);
  Log_info("Site %d data populated", site_info_->id);
  verify(ret > 0);
}

void ServerWorker::SetupService() {
  int ret;
  // set running mode and initialize transaction manager.
  std::string bind_addr = site_info_->GetBindAddress();

//  if (0 != (ret = Config::GetConfig()->get_my_addr(bind_addr))) {
//    verify(0);
//  }

  // init service implement
  rsi_g = new RococoServiceImpl(txn_mgr_, scsi_g);

  // init rrr::PollMgr 1 threads
  int n_io_threads = 1;
  svr_poll_mgr_g = new rrr::PollMgr(n_io_threads);

  auto &alarm = TimeoutALock::get_alarm_s();
  ServerWorker::svr_poll_mgr_g->add(&alarm);

  // TODO replace below with set_stat
  auto &recorder = ((RococoServiceImpl *) ServerWorker::rsi_g)->recorder_;

  if (recorder != NULL) {
    svr_poll_mgr_g->add(recorder);
  }

  if (scsi_g) {
    scsi_g->set_recorder(recorder);
    scsi_g->set_stat(ServerControlServiceImpl::STAT_SZ_SCC,
                     &rsi_g->stat_sz_scc_);
    scsi_g->set_stat(ServerControlServiceImpl::STAT_SZ_GRAPH_START,
                     &rsi_g->stat_sz_gra_start_);
    scsi_g->set_stat(ServerControlServiceImpl::STAT_SZ_GRAPH_COMMIT,
                     &ServerWorker::rsi_g->stat_sz_gra_commit_);
    scsi_g->set_stat(ServerControlServiceImpl::STAT_SZ_GRAPH_ASK,
                     &rsi_g->stat_sz_gra_ask_);
    scsi_g->set_stat(ServerControlServiceImpl::STAT_N_ASK,
                     &rsi_g->stat_n_ask_);
    scsi_g->set_stat(ServerControlServiceImpl::STAT_RO6_SZ_VECTOR,
                     &rsi_g->stat_ro6_sz_vector_);
  }

  // init base::ThreadPool
  uint32_t num_threads = 1;
  thread_pool_g = new base::ThreadPool(num_threads);

  // init rrr::Server
  svr_server_g = new rrr::Server(svr_poll_mgr_g, thread_pool_g);

  // reg service
  svr_server_g->reg(rsi_g);

  // start rpc server
  ret = svr_server_g->start(bind_addr.c_str());
  if (ret != 0) {
    Log_fatal("server launch failed.");
  }


  Log_info("Server ready");


  if (svr_hb_server_g != nullptr) {
#ifdef CPU_PROFILE
    char prof_file[1024];
    verify(0 < Config::get_config()->get_prof_filename(prof_file));

    // start to profile
    ProfilerStart(prof_file);
#endif // ifdef CPU_PROFILE
    scsi_g->set_ready();
    scsi_g->wait_for_shutdown();
    delete svr_hb_server_g;
    delete scsi_g;
    svr_hb_poll_mgr_g->release();
    hb_thread_pool_g->release();

    auto &recorder = ((DepTranServiceImpl *) rsi_g)->recorder_;
    if (recorder) {
      auto n_flush_avg_ = recorder->stat_cnt_.peek().avg_;
      auto sz_flush_avg_ = recorder->stat_sz_.peek().avg_;
      Log::info("Log to disk, average log per flush: %lld,"
                    " average size per flush: %lld",
                n_flush_avg_, sz_flush_avg_);
    }
#ifdef CPU_PROFILE

    // stop profiling
    ProfilerStop();
#endif // ifdef CPU_PROFILE
  }
//  else {
//    while (1) {
//      sleep(1000);
//    }
//  }

  Log::info("asking other server finish request count: %d",
            ((DepTranServiceImpl *) rsi_g)->n_asking_);

  // TODO move this to somewhere else
//  delete svr_server_g;
//  delete rsi_g;
//  thread_pool_g->release();
//  svr_poll_mgr_g->release();
//  delete DTxnMgr::get_sole_mgr();
//  RandomGenerator::destroy();
//  Config::DestroyConfig();
}

} // namespace rococo