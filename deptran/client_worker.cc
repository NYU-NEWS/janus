#include "client_worker.h"
#include "frame.h"
#include "txn_chopper.h"
#include "coordinator.h"
#include "piece.h"
#include "benchmark_control_rpc.h"

namespace rococo {

ClientWorker::~ClientWorker() {
  if (txn_generator_) {
    delete txn_generator_;
  }
  for (auto c : coos_) {
    delete c;
  }
  coos_.clear();
}

void ClientWorker::RequestDone(Coordinator* coo, TxnReply &txn_reply) {
  verify(coo != nullptr);

  if (txn_reply.res_ == SUCCESS)
    success++;
  num_txn++;
  num_try.fetch_add(txn_reply.n_try_);

  bool have_more_time = timer_->elapsed() < duration;

  Log_debug("elapsed: %2.2f; duration: %d", timer_->elapsed(), duration);
  if (have_more_time && config_->client_type_ == Config::Open) {
    finish_mutex.lock();
    n_concurrent_--;
    Log_debug("open client -- num_outstanding: %d", n_concurrent_);
    finish_mutex.unlock();
  } else if (have_more_time && config_->client_type_ == Config::Closed) {
    Log_debug("there is still time to issue another request. continue.");
    DispatchRequest(coo);
  } else if (!have_more_time) {
    Log_debug("times up. stop.");
    Log_debug("n_concurrent_ = %d", n_concurrent_);
    finish_mutex.lock();
    n_concurrent_--;
    if (n_concurrent_ == 0) {
      Log_debug("signal...");
      finish_cond.signal();
    }
    finish_mutex.unlock();
  }
}

Coordinator* ClientWorker::CreateCoordinator(int offset_id) {
  cooid_t coo_id = cli_id_;
  coo_id = (coo_id << 16) + offset_id;
  auto coo = frame_->CreateCoord(coo_id,
                                 config_,
                                 benchmark,
                                 ccsi,
                                 id,
                                 txn_reg_);
  coo->loc_id_ = my_site_.locale_id;
  coo->commo_ = commo_;
  Log_debug("coordinator %d created.", coo_id);
  return coo;
}

void ClientWorker::work() {
  Log_debug("%s", __FUNCTION__);
  verify(coos_.size() == 0);
  txn_reg_ = new TxnRegistry();
  Piece *piece = Piece::get_piece(benchmark);
  piece->txn_reg_ = txn_reg_;
  piece->reg_all();

  if (ccsi) ccsi->wait_for_start(id);
  Log_debug("after wait for start");

  timer_ = new Timer();
  timer_->start();

  if (config_->client_type_ == Config::Closed) {
    verify(n_concurrent_ > 0);
    for (uint32_t n_txn = 0; n_txn < n_concurrent_; n_txn++) {
      auto coo = CreateCoordinator(n_txn);
      DispatchRequest(coo);
    }
  } else {
    const double wait_time = 1.0/(double)config_->client_rate_;
    Log_debug("wait time %2.2f", wait_time);
    unsigned long int txn_count = 0;
    std::function<void()> do_dispatch = [&]() {
      double tps=0;
      int count=0;
      do {
        txn_count++;
        tps = txn_count / this->timer_->elapsed();
        count++;
        auto coo = this->CreateCoordinator(txn_count);
        this->DispatchRequest(coo);
        Log_debug("client tps: %2.2f", tps);
      } while (tps < this->config_->client_rate_);
    };

    n_concurrent_ = 0;
    while (timer_->elapsed() < duration) {
      finish_mutex.lock();
      n_concurrent_++;
      finish_mutex.unlock();
      do_dispatch();
      std::this_thread::sleep_for(std::chrono::duration<double>(wait_time));
    }
    Log_debug("exit client dispatch loop...");
  }

  finish_mutex.lock();
  while (n_concurrent_ > 0) {
    Log_debug("wait for finish... %d", n_concurrent_);
    finish_cond.wait(finish_mutex);
  }
  finish_mutex.unlock();
  fflush(stderr);
  fflush(stdout);

  Log_info("Finish:\nTotal: %u, Commit: %u, Attempts: %u, Running for %u\n",
           num_txn.load(),
           success.load(),
           num_try.load(),
           Config::GetConfig()->get_duration());
  fflush(stderr);
  fflush(stdout);
  if (ccsi) {
    Log_info("%s: wait_for_shutdown at client %d", __FUNCTION__, cli_id_);
    ccsi->wait_for_shutdown();
  }
  delete timer_;
  return;
}

  void ClientWorker::DispatchRequest(Coordinator *coo) {
    TxnRequest req;
    txn_generator_->GetTxnReq(&req, coo->coo_id_);
    req.callback_ = std::bind(&rococo::ClientWorker::RequestDone,
                              this,
                              coo,
                              std::placeholders::_1);
    coos_.push_back(coo);
    coo->do_one(req);
  }

  ClientWorker::ClientWorker(uint32_t id,
                           Config::SiteInfo &site_info,
                           Config *config,
                           ClientControlServiceImpl *ccsi)
    : id(id),
      my_site_(site_info),
      config_(config),
      cli_id_(site_info.id),
      benchmark(config->get_benchmark()),
      mode(config->get_mode()),
      duration(config->get_duration()),
      ccsi(ccsi),
      n_concurrent_(config->get_concurrent_txn()) {
  frame_ = Frame::GetFrame(config->cc_mode_);
  txn_generator_ = frame_->CreateTxnGenerator();
  config->get_all_site_addr(servers_);
  num_txn.store(0);
  success.store(0);
  num_try.store(0);
  commo_ = frame_->CreateCommo();
  commo_->loc_id_ = my_site_.locale_id;
}

} // namespace rococo


