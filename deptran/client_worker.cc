#include <cmath>
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
  for (auto c : created_coordinators_) {
    delete c;
  }
  dispatch_pool_->release();
}

void ClientWorker::ForwardRequestDone(Coordinator* coo,TxnReply* output, rrr::DeferredReply* defer, TxnReply &txn_reply) {
  verify(coo != nullptr);
  verify(output != nullptr);

  *output = txn_reply;

  bool have_more_time = timer_->elapsed() < duration;
  if (have_more_time && config_->client_type_ == Config::Open) {
    std::lock_guard<std::mutex> lock(coordinator_mutex);
    coo->forward_status_ = NONE;
    free_coordinators_.push_back(coo);
  } else if (!have_more_time) {
    Log_debug("times up. stop.");
    Log_debug("n_concurrent_ = %d", n_concurrent_);
    finish_mutex.lock();
    n_concurrent_--;
    if (n_concurrent_ == 0) {
      Log_debug("all coordinators finished... signal done");
      finish_cond.signal();
    } else {
      Log_debug("waiting for %d more coordinators to finish", n_concurrent_);
    }
    finish_mutex.unlock();
  }

  defer->reply();
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
    std::lock_guard<std::mutex> lock(coordinator_mutex);
    free_coordinators_.push_back(coo);
  } else if (have_more_time && config_->client_type_ == Config::Closed) {
    Log_debug("there is still time to issue another request. continue.");
    DispatchRequest(coo);
  } else if (!have_more_time) {
    Log_debug("times up. stop.");
    Log_debug("n_concurrent_ = %d", n_concurrent_);
    finish_mutex.lock();
    n_concurrent_--;
    if (n_concurrent_ == 0) {
      Log_debug("all coordinators finished... signal done");
      finish_cond.signal();
    } else {
      Log_debug("waiting for %d more coordinators to finish", n_concurrent_);
    }
    finish_mutex.unlock();
  }
}


Coordinator* ClientWorker::FindOrCreateCoordinator() {
  std::lock_guard<std::mutex> lock(coordinator_mutex);

  Coordinator* coo = nullptr;

  if (free_coordinators_.size() > 0) {
    coo = dynamic_cast<Coordinator*>(free_coordinators_.back());
    free_coordinators_.pop_back();
  } else {
    if (created_coordinators_.size() == UINT16_MAX) {
      return nullptr;
    }
    verify(created_coordinators_.size() <= UINT16_MAX);
    coo = CreateCoordinator(created_coordinators_.size());
  }

  return coo;
}


Coordinator* ClientWorker::CreateCoordinator(uint16_t offset_id) {

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
  coo->forward_status_ = forward_requests_to_leader_ ? FORWARD_TO_LEADER : NONE;
  Log_info("coordinator %d created at site %d: forward %d", coo->coo_id_, this->my_site_.id, coo->forward_status_);
  created_coordinators_.push_back(coo);
  return coo;
}

void ClientWorker::work() {
  Log_debug("%s: %d", __FUNCTION__, this->cli_id_);
  txn_reg_ = new TxnRegistry();
  Piece *piece = Piece::get_piece(benchmark);
  piece->txn_reg_ = txn_reg_;
  piece->reg_all();

  commo_->WaitConnectClientLeaders();
  if (ccsi) {
    ccsi->wait_for_start(id);
  }
  Log_debug("after wait for start");

  timer_ = new Timer();
  timer_->start();

  if (config_->client_type_ == Config::Closed) {
    Log_info("closed loop clients.");
    verify(n_concurrent_ > 0);
    for (uint32_t n_txn = 0; n_txn < n_concurrent_; n_txn++) {
      auto coo = CreateCoordinator(n_txn);
      Log_debug("create coordinator %d", coo->coo_id_);
      DispatchRequest(coo);
    }
  } else {
    Log_info("open loop clients.");
    const std::chrono::nanoseconds wait_time
        ((int)(pow(10,9) * 1.0/(double)config_->client_rate_));
    double tps = 0;
    long txn_count = 0;
    auto start = std::chrono::steady_clock::now();
    std::chrono::nanoseconds elapsed;

    while (timer_->elapsed() < duration) {
      while (tps < config_->client_rate_ && timer_->elapsed() < duration) {
        auto coo = FindOrCreateCoordinator();
        if (coo != nullptr) {
          DispatchRequest(coo);
          txn_count++;
          elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>
              (std::chrono::steady_clock::now() - start);
          tps = (double) txn_count / elapsed.count() * pow(10, 9);
        }
      }
      auto next_time = std::chrono::steady_clock::now() + wait_time;
      std::this_thread::sleep_until(next_time);
      elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>
          (std::chrono::steady_clock::now() - start);
      tps = (double)txn_count / elapsed.count() * pow(10,9);
    }
    Log_debug("exit client dispatch loop...");
  }

  finish_mutex.lock();
  while (n_concurrent_ > 0) {
    Log_debug("wait for finish... %d", n_concurrent_);
    finish_cond.wait(finish_mutex);
  }
  finish_mutex.unlock();

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


void ClientWorker::AcceptForwardedRequest(TxnRequest &request, TxnReply* txn_reply, rrr::DeferredReply* defer) {
  const char* f = __FUNCTION__;

  // obtain free a coordinator first
  Coordinator *coo = nullptr;
  while (coo == nullptr) {
    coo = FindOrCreateCoordinator();
  }
  coo->forward_status_ = PROCESS_FORWARD_REQUEST;

  // run the task
  std::function<void()> task = [=]() {
      TxnRequest req(request);
      req.callback_ = std::bind(&rococo::ClientWorker::ForwardRequestDone,
                                    this,
                                    coo,
                                    txn_reply,
                                    defer,
                                    std::placeholders::_1);
      Log_debug("%s: running forwarded request at site %d", f, my_site_.id);
      coo->do_one(req);
  };
  dispatch_pool_->run_async(task);
}

void ClientWorker::DispatchRequest(Coordinator *coo) {
    const char* f = __FUNCTION__;
    std::function<void()> task = [=]() {
      Log_info("%s: %d", f, cli_id_);
      TxnRequest req;
      {
        std::lock_guard<std::mutex> lock(this->request_gen_mutex);
        txn_generator_->GetTxnReq(&req, coo->coo_id_);
      }
      req.callback_ = std::bind(&rococo::ClientWorker::RequestDone,
                                this,
                                coo,
                                std::placeholders::_1);
      coo->do_one(req);
    };
    dispatch_pool_->run_async(task);
}

ClientWorker::ClientWorker(
    uint32_t id,
    Config::SiteInfo &site_info,
    Config *config,
    ClientControlServiceImpl *ccsi) :
    id(id),
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
  forward_requests_to_leader_ = (config->ab_mode_ == MODE_MULTI_PAXOS && site_info.locale_id != 0) ? true : false;
  Log_debug("client %d created; forward %d", cli_id_, forward_requests_to_leader_);
}

} // namespace rococo


