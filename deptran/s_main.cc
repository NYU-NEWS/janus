#include "all.h"
#ifdef CPU_PROFILE
# include <google/profiler.h>
#endif // ifdef CPU_PROFILE

using namespace rococo;

extern rrr::PollMgr *poll_mgr_g;
static ServerControlServiceImpl *scsi_g = NULL;
static rrr::PollMgr *hb_poll_mgr_g = NULL;
static rrr::Server *hb_server_g = NULL;
static base::ThreadPool *hb_thread_pool_g = NULL;

static void server_pop_table() {
  // populate table
  auto sid = Config::get_config()->get_site_id();
  int ret = 0;

  // get all tables
  std::vector<std::string> table_names;

  if (0 >= (ret = Sharding::get_table_names(sid, table_names))) verify(0);

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
    DTxnMgr::get_sole_mgr()->reg_table(*table_it, tb);
  }
  Sharding::populate_table(table_names, sid);
  verify(ret > 0);
}

static void server_reg_piece() {
  auto benchmark = Config::get_config()->get_benchmark();
  Piece *piece = Piece::get_piece(benchmark);
  piece->reg_all();
  delete piece;
  piece = NULL;
}

void server_setup_heartbeat() {
  bool hb = Config::get_config()->do_heart_beat();
  if (!hb) return;
  auto timeout = Config::get_config()->get_ctrl_timeout();
  scsi_g = new ServerControlServiceImpl(timeout);
  int n_io_threads = 1;
  hb_poll_mgr_g = new rrr::PollMgr(n_io_threads);
  hb_thread_pool_g = new rrr::ThreadPool(1);
  hb_server_g = new rrr::Server(hb_poll_mgr_g, hb_thread_pool_g);
  hb_server_g->reg(scsi_g);
  auto port = Config::get_config()->get_ctrl_port();
  std::string addr_port = std::string("0.0.0.0:") +
      std::to_string(port);
  hb_server_g->start(addr_port.c_str());
}

void server_setup_service() {
  int ret;
  // set running mode and initialize transaction manager.
  int running_mode = Config::get_config()->get_mode();
  DTxnMgr *mgr = new DTxnMgr(running_mode);
  std::string bind_addr;

  if (0 != (ret = Config::get_config()->get_my_addr(bind_addr))) {
    verify(0);
  }

  // init service implement
  auto *rococo_service = new RococoServiceImpl(mgr, scsi_g);

  // init rrr::PollMgr 1 threads
  int n_io_threads = 1;
  poll_mgr_g = new rrr::PollMgr(n_io_threads);

  auto &alarm = TimeoutALock::get_alarm_s();
  poll_mgr_g->add(&alarm);

  // TODO replace below with set_stat
  auto &recorder = ((RococoServiceImpl *) rococo_service)->recorder_;

  if (recorder != NULL) {
    poll_mgr_g->add(recorder);
  }

  if (scsi_g) {
    scsi_g->set_recorder(recorder);
    scsi_g->set_stat(ServerControlServiceImpl::STAT_SZ_SCC,
                     &rococo_service->stat_sz_scc_);
    scsi_g->set_stat(ServerControlServiceImpl::STAT_SZ_GRAPH_START,
                     &rococo_service->stat_sz_gra_start_);
    scsi_g->set_stat(ServerControlServiceImpl::STAT_SZ_GRAPH_COMMIT,
                     &rococo_service->stat_sz_gra_commit_);
    scsi_g->set_stat(ServerControlServiceImpl::STAT_SZ_GRAPH_ASK,
                     &rococo_service->stat_sz_gra_ask_);
    scsi_g->set_stat(ServerControlServiceImpl::STAT_N_ASK,
                     &rococo_service->stat_n_ask_);
    scsi_g->set_stat(ServerControlServiceImpl::STAT_RO6_SZ_VECTOR,
                     &rococo_service->stat_ro6_sz_vector_);
  }

  // init base::ThreadPool
  uint32_t num_threads;

  if (0 != (ret = Config::get_config()->get_threads(num_threads))) {
    verify(0);
  }
  base::ThreadPool *thread_pool = new base::ThreadPool(num_threads);

  // init rrr::Server
  rrr::Server *server = new rrr::Server(poll_mgr_g, thread_pool);

  // reg service
  server->reg(rococo_service);

  // start rpc server
  server->start(bind_addr.c_str());

  Log_info("Server ready");


  if (hb_server_g != nullptr) {
#ifdef CPU_PROFILE
    char prof_file[1024];
    verify(0 < Config::get_config()->get_prof_filename(prof_file));

    // start to profile
    ProfilerStart(prof_file);
#endif // ifdef CPU_PROFILE
    scsi_g->set_ready();
    scsi_g->wait_for_shutdown();
    delete hb_server_g;
    delete scsi_g;
    hb_poll_mgr_g->release();
    hb_thread_pool_g->release();

    auto &recorder = ((DepTranServiceImpl *) rococo_service)->recorder_;

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
  else {
    while (1) {
      sleep(1000);
    }
  }

  Log::info("asking other server finish request count: %d",
            ((DepTranServiceImpl *) rococo_service)->n_asking_);

  delete server;
  delete rococo_service;
  thread_pool->release();
  poll_mgr_g->release();
  delete DTxnMgr::get_sole_mgr();
  RandomGenerator::destroy();
  Config::destroy_config();
}


int main(int argc, char *argv[]) {
  int ret;

  // read configuration
  if (0 != (ret = Config::create_config(argc, argv))) {
    Log_fatal("Read config failed");
    return ret;
  }
  // setup communication between controller script
  server_setup_heartbeat();
  // populate table according to benchmarks
  server_pop_table();
  // register txn piece logic
  server_reg_piece();
  // start server service
  server_setup_service();
  return 0;
}
