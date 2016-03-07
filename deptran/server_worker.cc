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
  hb_rpc_server_ = new rrr::Server(svr_hb_poll_mgr_g, hb_thread_pool_g);
  hb_rpc_server_->reg(scsi_g);
  auto port = Config::GetConfig()->get_ctrl_port();
  std::string addr_port = std::string("0.0.0.0:") +
      std::to_string(port);
  hb_rpc_server_->start(addr_port.c_str());
}

void ServerWorker::SetupBase() {
  auto config = Config::GetConfig();
  dtxn_frame_ = Frame::GetFrame(config->cc_mode_);
  dtxn_frame_->site_info_ = site_info_;

  // this needs to be done before poping table
  sharding_ = dtxn_frame_->CreateSharding(Config::GetConfig()->sharding_);
  sharding_->BuildTableInfoPtr();

  verify(txn_reg_ == nullptr);
  txn_reg_ = new TxnRegistry();
  dtxn_sched_ = dtxn_frame_->CreateScheduler();
  dtxn_sched_->txn_reg_ = txn_reg_;
  sharding_->dtxn_sched_ = dtxn_sched_;

  if (config->IsReplicated() &&
      config->ab_mode_ != config->cc_mode_) {
    rep_frame_ = Frame::GetFrame(config->ab_mode_);
    rep_frame_->site_info_ = site_info_;
    rep_sched_ = rep_frame_->CreateScheduler();
    rep_sched_->txn_reg_ = txn_reg_;
    dtxn_sched_->rep_frame_ = rep_frame_;
    dtxn_sched_->rep_sched_ = rep_sched_;
  }
}

void ServerWorker::PopTable() {
  // populate table
  int ret = 0;
  // get all tables
  std::vector<std::string> table_names;

  ret = sharding_->GetTableNames(site_info_->id, table_names);
  verify(ret > 0);

  for (auto table_name : table_names) {
    mdb::Schema *schema = new mdb::Schema();
    mdb::symbol_t symbol;
    sharding_->init_schema(table_name, schema, &symbol);
    mdb::Table *tb;

    switch (symbol) {
      case mdb::TBL_SORTED:
        tb = new mdb::SortedTable(table_name, schema);
        break;
      case mdb::TBL_UNSORTED:
        tb = new mdb::UnsortedTable(table_name, schema);
        break;
      case mdb::TBL_SNAPSHOT:
        tb = new mdb::SnapshotTable(table_name, schema);
        break;
      default:
        verify(0);
    }
    dtxn_sched_->reg_table(table_name, tb);
  }
  verify(sharding_);
  sharding_->PopulateTable(site_info_->partition_id_);
  Log_info("Site %d data populated", site_info_->id);
  verify(ret > 0);
}

void ServerWorker::RegPiece() {
  auto benchmark = Config::GetConfig()->get_benchmark();
  Piece *piece = Piece::get_piece(benchmark);
  verify(txn_reg_ != nullptr);
  verify(sharding_ != nullptr);
  piece->sss_ = sharding_;
  piece->txn_reg_ = txn_reg_;
  piece->reg_all();
}

void ServerWorker::SetupService() {
  int ret;
  // set running mode and initialize transaction manager.
  std::string bind_addr = site_info_->GetBindAddress();

  // init rrr::PollMgr 1 threads
  int n_io_threads = 1;
  svr_poll_mgr_g = new rrr::PollMgr(n_io_threads);

  // init service implementation

  if (dtxn_frame_ != nullptr) {
    services_ = dtxn_frame_->CreateRpcServices(site_info_->id,
                                               dtxn_sched_,
                                               svr_poll_mgr_g,
                                               scsi_g);
  }

  if (rep_frame_ != nullptr) {
    auto s2 = rep_frame_->CreateRpcServices(site_info_->id,
                                            rep_sched_,
                                            svr_poll_mgr_g,
                                            scsi_g);

    services_.insert(services_.end(), s2.begin(), s2.end());
  }

  auto &alarm = TimeoutALock::get_alarm_s();
  ServerWorker::svr_poll_mgr_g->add(&alarm);

  uint32_t num_threads = 1;
  thread_pool_g = new base::ThreadPool(num_threads);

  // init rrr::Server
  rpc_server_ = new rrr::Server(svr_poll_mgr_g, thread_pool_g);

  // reg services
  for (auto service : services_) {
    rpc_server_->reg(service);
  }

  // start rpc server
  ret = rpc_server_->start(bind_addr.c_str());
  if (ret != 0) {
    Log_fatal("server launch failed.");
  }

  Log_info("Server ready");

  if (hb_rpc_server_ != nullptr) {
#ifdef CPU_PROFILE
    char prof_file[1024];
    verify(0 < Config::get_config()->get_prof_filename(prof_file));

    // start to profile
    ProfilerStart(prof_file);
#endif // ifdef CPU_PROFILE
    scsi_g->set_ready();
    scsi_g->wait_for_shutdown();
    delete hb_rpc_server_;
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
//        Log::info("asking other server finish request count: %d", s->n_asking_);
      }
    }
#ifdef CPU_PROFILE
    // stop profiling
    ProfilerStop();
#endif // ifdef CPU_PROFILE
  }
}

void ServerWorker::SetupCommo() {
  if (dtxn_frame_) {
    dtxn_commo_ = dtxn_frame_->CreateCommo();
  }
  if (rep_frame_) {
    rep_commo_ = rep_frame_->CreateCommo();
  }
}

void ServerWorker::ShutDown() {
  // TODO server would not delete?
  Log_debug("deleting services, num: %d", services_.size());
  delete rpc_server_;
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