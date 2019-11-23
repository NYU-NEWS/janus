#include "paxos_worker.h"
#include "service.h"

#include "classic/tpc_command.h"

namespace janus {

static int volatile xx =
    MarshallDeputy::RegInitializer(MarshallDeputy::CONTAINER_CMD,
                                   []() -> Marshallable* {
                                     return new CmdData;
                                   });

void PaxosWorker::SetupBase() {
  auto config = Config::GetConfig();
  rep_frame_ = Frame::GetFrame(config->replica_proto_);
  Log_debug("################# replica_proto_: %d", config->replica_proto_);
  rep_frame_->site_info_ = site_info_;
  rep_sched_ = rep_frame_->CreateScheduler();
  // rep_sched_->txn_reg_ = tx_reg_;
  rep_sched_->loc_id_ = site_info_->locale_id;
}

void PaxosWorker::Next(Marshallable& cmd) {
  finish_mutex.lock();
  n_current--;
  if (n_current == 0) {
    finish_cond.signal();
  }
  finish_mutex.unlock();
}

void PaxosWorker::SetupService() {
  std::string bind_addr = site_info_->GetBindAddress();
  int n_io_threads = 1;
  svr_poll_mgr_ = new rrr::PollMgr(n_io_threads);
  if (rep_frame_ != nullptr) {
    services_ = rep_frame_->CreateRpcServices(site_info_->id,
                                              rep_sched_,
                                              svr_poll_mgr_,
                                              scsi_);
  }
  uint32_t num_threads = 1;
  thread_pool_g = new base::ThreadPool(num_threads);

  // init rrr::Server
  rpc_server_ = new rrr::Server(svr_poll_mgr_, thread_pool_g);

  // reg services
  for (auto service : services_) {
    rpc_server_->reg(service);
  }

  // start rpc server
  Log_debug("starting server at %s", bind_addr.c_str());
  int ret = rpc_server_->start(bind_addr.c_str());
  if (ret != 0) {
    Log_fatal("server launch failed.");
  }

  Log_info("Server %s ready at %s",
           site_info_->name.c_str(),
           bind_addr.c_str());
}

void PaxosWorker::SetupCommo() {
  verify(svr_poll_mgr_ != nullptr);
  if (rep_frame_) {
    rep_commo_ = rep_frame_->CreateCommo();
    if (rep_commo_) {
      rep_commo_->loc_id_ = site_info_->locale_id;
    }
    rep_sched_->commo_ = rep_commo_;
  }
}

void PaxosWorker::SetupHeartbeat() {
  bool hb = Config::GetConfig()->do_heart_beat();
  if (!hb) return;
  auto timeout = Config::GetConfig()->get_ctrl_timeout();
  scsi_ = new ServerControlServiceImpl(timeout);
  int n_io_threads = 1;
  svr_hb_poll_mgr_g = new rrr::PollMgr(n_io_threads);
  hb_thread_pool_g = new rrr::ThreadPool(1);
  hb_rpc_server_ = new rrr::Server(svr_hb_poll_mgr_g, hb_thread_pool_g);
  hb_rpc_server_->reg(scsi_);

  auto port = site_info_->port + CtrlPortDelta;
  std::string addr_port = std::string("0.0.0.0:") +
                          std::to_string(port);
  hb_rpc_server_->start(addr_port.c_str());
  if (hb_rpc_server_ != nullptr) {
    // Log_info("notify ready to control script for %s", bind_addr.c_str());
    scsi_->set_ready();
  }
  Log_info("heartbeat setup for %s on %s",
           site_info_->name.c_str(), addr_port.c_str());
}

void PaxosWorker::WaitForShutdown() {
  Log_debug("enter in %s", __FUNCTION__);

  if (hb_rpc_server_ != nullptr) {
    scsi_->server_shutdown();
    delete hb_rpc_server_;
    delete scsi_;
    svr_hb_poll_mgr_g->release();
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

void PaxosWorker::ShutDown() {
  Log_debug("par_id #%d loc_id #%d deleting services, num: %d", site_info_->partition_id_, site_info_->locale_id, services_.size());
  delete rpc_server_;
  for (auto service : services_) {
    delete service;
  }
  thread_pool_g->release();
}

void PaxosWorker::Submit(shared_ptr<Marshallable> sp_m) {
  finish_mutex.lock();
  n_current++;
  finish_mutex.unlock();
  static cooid_t cid = 0;
  int32_t benchmark = 0;
  static id_t id = 0;
  verify(rep_frame_ != nullptr);
  Coordinator* coord = rep_frame_->CreateCoordinator(cid++,
                                        Config::GetConfig(),
                                        benchmark,
                                        nullptr,
                                        id++,
                                        nullptr);
  coord->par_id_ = site_info_->partition_id_;
  coord->loc_id_ = site_info_->locale_id;
  coord->Submit(sp_m);
  created_coordinators_.push_back(coord);
}

void PaxosWorker::SubmitExample() {
  // TODO
  auto sp_tx = make_shared<Tx>(100, 1, rep_sched_);
  sp_tx->cmd_ = make_shared<CmdData>();

  auto sp_prepare_cmd = std::make_shared<TpcPrepareCommand>();
  sp_prepare_cmd->tx_id_ = 0;
  sp_prepare_cmd->cmd_ = sp_tx->cmd_;
  auto sp_m = dynamic_pointer_cast<Marshallable>(sp_prepare_cmd);

  // -------------------callback-------------------
  rep_sched_->RegLearnerAction(std::bind(&PaxosWorker::Next,
                                         this,
                                         std::placeholders::_1));

  Submit(sp_m);

  finish_mutex.lock();
  while (n_current > 0) {
    Log_debug("wait for command replicated, amount: %d", n_current);
    finish_cond.wait(finish_mutex);
  }
  finish_mutex.unlock();
  Log_debug("finish command replicated.");
}

bool PaxosWorker::IsLeader(){
  verify(rep_frame_ != nullptr);
  verify(rep_frame_->site_info_ != nullptr);
  return rep_frame_->site_info_->locale_id == 0;
}

} // namespace janus