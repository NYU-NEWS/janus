#include "server_worker.h"
#include "rcc_service.h"
#include "benchmark_control_rpc.h"
#include "sharding.h"
#include "scheduler.h"
#include "frame.h"


namespace rococo {

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
  verify(txn_reg_);
  txn_mgr_ = Frame().CreateScheduler();
  txn_mgr_->txn_reg_ = txn_reg_;
  sharding_->dtxn_mgr_ = txn_mgr_;

  // populate table
  int ret = 0;

  // get all tables
  std::vector<std::string> table_names;

  ret = sharding_->get_table_names(site_info_->id, table_names);
  verify(ret > 0);

  for (auto table_name : table_names) {
    mdb::Schema *schema = new mdb::Schema();
    mdb::symbol_t symbol;
    sharding_->init_schema(table_name, schema, &symbol);
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
    txn_mgr_->reg_table(table_name, tb);
  }
  verify(sharding_);
  sharding_->PopulateTable(site_info_->partition_id_);
  Log_info("Site %d data populated", site_info_->id);
  verify(ret > 0);
}

void ServerWorker::RegPiece() {
  auto benchmark = Config::GetConfig()->get_benchmark();
  Piece *piece = Piece::get_piece(benchmark);
  verify(!txn_reg_);
  txn_reg_ = new TxnRegistry();
  verify(sharding_);
  piece->sss_ = sharding_;
  piece->txn_reg_ = txn_reg_;
  piece->reg_all();
}

void ServerWorker::SetupCommo() {
  commo_ = Frame().CreateCommo();
}

void ServerWorker::SetupService() {
  int ret;
  // set running mode and initialize transaction manager.
  std::string bind_addr = site_info_->GetBindAddress();

  // init rrr::PollMgr 1 threads
  int n_io_threads = 1;
  svr_poll_mgr_g = new rrr::PollMgr(n_io_threads);

  // init service implementation
  services_ = Frame().CreateRpcServices(site_info_->id,
                                        txn_mgr_,
                                        svr_poll_mgr_g,
                                        scsi_g);

  auto &alarm = TimeoutALock::get_alarm_s();
  ServerWorker::svr_poll_mgr_g->add(&alarm);

  uint32_t num_threads = 1;
  thread_pool_g = new base::ThreadPool(num_threads);

  // init rrr::Server
  svr_server_g = new rrr::Server(svr_poll_mgr_g, thread_pool_g);

  // reg services
  for (auto service : services_) {
    svr_server_g->reg(service);
  }

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

    for (auto service : services_) {
      if (DepTranServiceImpl* s = dynamic_cast<DepTranServiceImpl*>(service)) {
        auto &recorder = s->recorder_;
        if (recorder) {
          auto n_flush_avg_ = recorder->stat_cnt_.peek().avg_;
          auto sz_flush_avg_ = recorder->stat_sz_.peek().avg_;
          Log::info("Log to disk, average log per flush: %lld,"
                        " average size per flush: %lld",
                    n_flush_avg_, sz_flush_avg_);
        }
        Log::info("asking other server finish request count: %d", s->n_asking_);
      }
    }
#ifdef CPU_PROFILE
    // stop profiling
    ProfilerStop();
#endif // ifdef CPU_PROFILE
  }
}

void ServerWorker::ShutDown() {
  // TODO
  delete svr_server_g;
  for (auto service : services_) {
    delete service;
  }
  thread_pool_g->release();
  svr_poll_mgr_g->release();
//  delete DTxnMgr::get_sole_mgr();
//  RandomGenerator::destroy();
//  Config::DestroyConfig();
}
} // namespace rococo