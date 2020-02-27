#include "server_worker.h"
#include "service.h"
#include "benchmark_control_rpc.h"
#include "sharding.h"
#include "scheduler.h"
#include "frame.h"
#include "communicator.h"

namespace janus {

void ServerWorker::SetupHeartbeat() {
  bool hb = Config::GetConfig()->do_heart_beat();
  if (!hb) return;
  auto timeout = Config::GetConfig()->get_ctrl_timeout();
  scsi_ = new ServerControlServiceImpl(timeout);
  int n_io_threads = 1;
//  svr_hb_poll_mgr_g = new rrr::PollMgr(n_io_threads);
  svr_hb_poll_mgr_g = svr_poll_mgr_;
//  hb_thread_pool_g = new rrr::ThreadPool(1);
  hb_thread_pool_g = svr_thread_pool_;
  hb_rpc_server_ = new rrr::Server(svr_hb_poll_mgr_g, hb_thread_pool_g);
  hb_rpc_server_->reg(scsi_);

  auto port = this->site_info_->port + ServerWorker::CtrlPortDelta;
  std::string addr_port = std::string("0.0.0.0:") +
      std::to_string(port);
  hb_rpc_server_->start(addr_port.c_str());
  if (hb_rpc_server_ != nullptr) {
    // Log_info("notify ready to control script for %s", bind_addr.c_str());
    scsi_->set_ready();
  }
  Log_info("heartbeat setup for %s on %s",
           this->site_info_->name.c_str(), addr_port.c_str());
}

void ServerWorker::SetupBase() {
  auto config = Config::GetConfig();
  tx_frame_ = Frame::GetFrame(config->tx_proto_);
  tx_frame_->site_info_ = site_info_;

  // this needs to be done before poping table
  sharding_ = tx_frame_->CreateSharding(Config::GetConfig()->sharding_);
  sharding_->BuildTableInfoPtr();

  verify(tx_reg_ == nullptr);
  tx_reg_ = std::make_shared<TxnRegistry>();
  tx_sched_ = tx_frame_->CreateScheduler();
  tx_sched_->txn_reg_ = tx_reg_;
  tx_sched_->SetPartitionId(site_info_->partition_id_);
  tx_sched_->loc_id_ = site_info_->locale_id;
  tx_sched_->site_id_ = site_info_->id;
//  Log_info("initialize site id: %d", (int) site_info_->id);
  sharding_->tx_sched_ = tx_sched_;

  if (config->IsReplicated() &&
      config->replica_proto_ != config->tx_proto_) {
    rep_frame_ = Frame::GetFrame(config->replica_proto_);
    rep_frame_->site_info_ = site_info_;
    rep_sched_ = rep_frame_->CreateScheduler();
    rep_sched_->txn_reg_ = tx_reg_;
    rep_sched_->loc_id_ = site_info_->locale_id;
    tx_sched_->rep_frame_ = rep_frame_;
    tx_sched_->rep_sched_ = rep_sched_;
  }
  // add callbacks to execute commands to rep_sched_
  if (rep_sched_ && tx_sched_) {
    rep_sched_->RegLearnerAction(std::bind(&TxLogServer::Next,
                                           tx_sched_,
                                           std::placeholders::_1));
  }
}

void ServerWorker::PopTable() {
  // populate table
  int ret = 0;
  // get all tables
  std::vector<std::string> table_names;

  Log_info("start data population for site %d", site_info_->id);
  ret = sharding_->GetTableNames(site_info_->partition_id_, table_names);
  verify(ret > 0);

  for (auto table_name : table_names) {
    mdb::Schema* schema = new mdb::Schema();
    mdb::symbol_t symbol;
    sharding_->init_schema(table_name, schema, &symbol);
    mdb::Table* tb;

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
    tx_sched_->reg_table(table_name, tb);
  }
  verify(sharding_);
  sharding_->PopulateTables(site_info_->partition_id_);
  Log_info("data populated for site: %x, partition: %x",
           site_info_->id, site_info_->partition_id_);
  verify(ret > 0);
}

void ServerWorker::RegisterWorkload() {
  Workload* workload = Workload::CreateWorkload(Config::GetConfig());
  verify(tx_reg_ != nullptr);
  verify(sharding_ != nullptr);
  workload->sss_ = sharding_;
  workload->txn_reg_ = tx_reg_;
  workload->RegisterPrecedures();
}

void ServerWorker::SetupService() {
  Log_info("enter %s for %s @ %s", __FUNCTION__,
           this->site_info_->name.c_str(),
           site_info_->GetBindAddress().c_str());

  int ret;
  // set running mode and initialize transaction manager.
  std::string bind_addr = site_info_->GetBindAddress();

  // init rrr::PollMgr 1 threads
  int n_io_threads = 1;
  svr_poll_mgr_ = new rrr::PollMgr(n_io_threads);
//  svr_thread_pool_ = new rrr::ThreadPool(1);

  // init service implementation

  if (tx_frame_ != nullptr) {
    services_ = tx_frame_->CreateRpcServices(site_info_->id,
                                             tx_sched_,
                                             svr_poll_mgr_,
                                             scsi_);
  }

  if (rep_frame_ != nullptr) {
    auto s2 = rep_frame_->CreateRpcServices(site_info_->id,
                                            rep_sched_,
                                            svr_poll_mgr_,
                                            scsi_);

    services_.insert(services_.end(), s2.begin(), s2.end());
  }

//  auto& alarm = TimeoutALock::get_alarm_s();
//  ServerWorker::svr_poll_mgr_->add(&alarm);

  uint32_t num_threads = 1;
//  thread_pool_g = new base::ThreadPool(num_threads);

  // init rrr::Server
  rpc_server_ = new rrr::Server(svr_poll_mgr_, svr_thread_pool_);

  // reg services
  for (auto service : services_) {
    rpc_server_->reg(service);
  }

  // start rpc server
  Log_debug("starting server at %s", bind_addr.c_str());
  ret = rpc_server_->start(bind_addr.c_str());
  if (ret != 0) {
    Log_fatal("server launch failed.");
  }

  Log_info("Server %s ready at %s",
           site_info_->name.c_str(),
           bind_addr.c_str());

}

void ServerWorker::WaitForShutdown() {
  Log_debug("%s", __FUNCTION__);
  if (hb_rpc_server_ != nullptr) {
    scsi_->wait_for_shutdown();
    delete hb_rpc_server_;
    delete scsi_;
    if (svr_hb_poll_mgr_g != svr_poll_mgr_)
      svr_hb_poll_mgr_g->release();
    if (hb_thread_pool_g != svr_thread_pool_)
      hb_thread_pool_g->release();

    for (auto service : services_) {
      if (DepTranServiceImpl* s = dynamic_cast<DepTranServiceImpl*>(service)) {
        auto& recorder = s->recorder_;
        if (recorder) {
          auto n_flush_avg_ = recorder->stat_cnt_.peek().avg_;
          auto sz_flush_avg_ = recorder->stat_sz_.peek().avg_;
          Log::info("Log to disk, average log per flush: %lld,"
                        " average size per flush: %lld",
                    n_flush_avg_, sz_flush_avg_);
        }
      }
    }
  }
  Log_debug("exit %s", __FUNCTION__);
}

void ServerWorker::SetupCommo() {
  verify(svr_poll_mgr_ != nullptr);
  if (tx_frame_) {
    tx_commo_ = tx_frame_->CreateCommo(svr_poll_mgr_);
    if (tx_commo_) {
      tx_commo_->loc_id_ = site_info_->locale_id;
    }
    tx_sched_->commo_ = tx_commo_;
  }
  if (rep_frame_) {
    rep_commo_ = rep_frame_->CreateCommo(svr_poll_mgr_);
    if (rep_commo_) {
      rep_commo_->loc_id_ = site_info_->locale_id;
    }
    rep_sched_->commo_ = rep_commo_;

  }
}

void ServerWorker::ShutDown() {
  Log_debug("deleting services, num: %d", services_.size());
  delete rpc_server_;
  for (auto service : services_) {
    delete service;
  }
//  thread_pool_g->release();
  svr_poll_mgr_->release();
}
int ServerWorker::DbChecksum() {
  auto cs = this->tx_sched_->mdb_txn_mgr_->Checksum();
  Log_info("site_id: %d shard_id: %d checksum: %x", (int)this->site_info_->id,
           (int)this->site_info_->partition_id_, (int) cs);
  return cs;
}
} // namespace janus
