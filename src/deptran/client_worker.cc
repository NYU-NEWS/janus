#include <cmath>
#include "client_worker.h"
#include "frame.h"
#include "procedure.h"
#include "coordinator.h"
#include "workload.h"
#include "benchmark_control_rpc.h"

namespace janus {

ClientWorker::~ClientWorker() {
  if (tx_generator_) {
    delete tx_generator_;
  }
  for (auto c : created_coordinators_) {
    delete c;
  }
//  dispatch_pool_->release();
}

void ClientWorker::ForwardRequestDone(Coordinator* coo,
                                      TxReply* output,
                                      DeferredReply* defer,
                                      TxReply& txn_reply) {
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
//    finish_mutex.lock();
    n_concurrent_--;
    if (n_concurrent_ == 0) {
      Log_debug("all coordinators finished... signal done");
//      finish_cond.signal();
    } else {
      Log_debug("waiting for %d more coordinators to finish", n_concurrent_);
    }
//    finish_mutex.unlock();
  }

  defer->reply();
}



void ClientWorker::RequestDone(Coordinator* coo, TxReply& txn_reply) {
  verify(0);
  verify(coo != nullptr);

  if (txn_reply.res_ == SUCCESS)
    success++;
  num_txn++;
  num_try.fetch_add(txn_reply.n_try_);
  /*
  ssid_consistent.fetch_add(txn_reply.n_ssid_consistent_);
  decided.fetch_add(txn_reply.n_decided_);
  offset_valid.fetch_add(txn_reply.n_offset_valid_);
  validation_passed.fetch_add(txn_reply.n_validation_passed);
  cascading_aborts.fetch_add(txn_reply.n_cascading_aborts);
  single_shard.fetch_add(txn_reply.n_single_shard);
  single_shard_write_only.fetch_add(txn_reply.n_single_shard_write_only);
  */
  bool have_more_time = timer_->elapsed() < duration;
  Log_debug("received callback from tx_id %" PRIx64, txn_reply.tx_id_);
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
//    finish_mutex.lock();
    n_concurrent_--;
    verify(n_concurrent_ >= 0);
    if (n_concurrent_ == 0) {
      Log_debug("all coordinators finished... signal done");
//      finish_cond.signal();
    } else {
      Log_debug("waiting for %d more coordinators to finish", n_concurrent_);
      Log_debug("transactions they are processing:");
      // for debug purpose, print ongoing transaction ids.
      for (auto c : created_coordinators_) {
        if (c->ongoing_tx_id_ > 0) {
          Log_debug("\t %" PRIx64, c->ongoing_tx_id_);
        }
      }
    }
//    finish_mutex.unlock();
  } else {
    verify(0);
  }
}

Coordinator* ClientWorker::FindOrCreateCoordinator() {
  std::lock_guard<std::mutex> lock(coordinator_mutex);

  Coordinator* coo = nullptr;

  if (!free_coordinators_.empty()) {
    coo = dynamic_cast<Coordinator*>(free_coordinators_.back());
    free_coordinators_.pop_back();
  } else {
    if (created_coordinators_.size() == UINT16_MAX) {
      return nullptr;
    }
    verify(created_coordinators_.size() <= UINT16_MAX);
    coo = CreateCoordinator(created_coordinators_.size());
  }

  verify(!coo->_inuse_);
  coo->_inuse_ = true;
  return coo;
}

Coordinator* ClientWorker::CreateCoordinator(uint16_t offset_id) {

  cooid_t coo_id = cli_id_;
  coo_id = (coo_id << 16) + offset_id;
  auto coo = frame_->CreateCoordinator(coo_id,
                                       config_,
                                       benchmark,
                                       ccsi,
                                       id,
                                       txn_reg_);
  coo->loc_id_ = my_site_.locale_id;
  coo->commo_ = commo_;
  coo->forward_status_ = forward_requests_to_leader_ ? FORWARD_TO_LEADER : NONE;
  Log_debug("coordinator %d created at site %d: forward %d",
            coo->coo_id_,
            this->my_site_.id,
            coo->forward_status_);
  created_coordinators_.push_back(coo);
  return coo;
}

void ClientWorker::Work() {
  Log_debug("%s: %d", __FUNCTION__, this->cli_id_);
  txn_reg_ = std::make_shared<TxnRegistry>();
  verify(config_ != nullptr);
  Workload* workload = Workload::CreateWorkload(config_);
  workload->txn_reg_ = txn_reg_;
  workload->RegisterPrecedures();

  commo_->WaitConnectClientLeaders();
  if (ccsi) {
    ccsi->wait_for_start(id);
  }
  Log_debug("after wait for start");


  for (uint32_t n_tx = 0; n_tx < n_concurrent_; n_tx++) {
    auto sp_job = std::make_shared<OneTimeJob>([this] () {
      // this wait tries to avoid launching clients all at once, especially for open-loop clients.
      Reactor::CreateSpEvent<NeverEvent>()->Wait(RandomGenerator::rand(0, 1000000));
      auto beg_time = Time::now() ;
      auto end_time = beg_time + duration * pow(10, 6);
      while (true) {
        auto cur_time = Time::now(); // optimize: this call is not scalable.
        if (cur_time > end_time) {
          break;
        }
        n_tx_issued_++;
          while (true) {
              auto n_undone_tx = n_tx_issued_ - sp_n_tx_done_.value_;
              if (n_undone_tx % 1000 == 0) {
                  Log_debug("unfinished tx %d", n_undone_tx);
              }
              if (config_->client_max_undone_ > 0 && n_undone_tx > config_->client_max_undone_) {
                  Reactor::CreateSpEvent<NeverEvent>()->Wait(pow(10, 4));
              } else {
                  break;
              }
          }
        num_txn++;
        auto coo = FindOrCreateCoordinator();
        verify(!coo->sp_ev_commit_);
        verify(!coo->sp_ev_done_);
        coo->sp_ev_commit_ = Reactor::CreateSpEvent<IntEvent>();
        coo->sp_ev_done_ = Reactor::CreateSpEvent<IntEvent>();
        this->DispatchRequest(coo);
        if (config_->client_type_ == Config::Closed) {
          auto ev = coo->sp_ev_commit_;
          ev->Wait();
//          ev->Wait(300*1000*1000);
          verify(ev->status_ != Event::TIMEOUT);
        } else {
          auto sp_event = Reactor::CreateSpEvent<NeverEvent>();
          sp_event->Wait(pow(10, 6));
        }
        Coroutine::CreateRun([this, coo](){
          verify(coo->_inuse_);
          auto ev = coo->sp_ev_done_;
          ev->Wait();
          verify(coo->coo_id_ > 0);
//          ev->Wait(400*1000*1000);
          verify(coo->_inuse_);
          verify(coo->coo_id_ > 0);
          verify(ev->status_ != Event::TIMEOUT);
          if (coo->committed_) {
            success++;
          }

          // LFC related stats
          ssid_consistent.fetch_add(coo->n_ssid_consistent_);
          decided.fetch_add(coo->n_decided_);
          validation_passed.fetch_add(coo->n_validation_passed);
          cascading_aborts.fetch_add(coo->n_cascading_aborts);
          rotxn_aborts.fetch_add(coo->n_rotxn_aborts);
          early_aborts.fetch_add(coo->n_early_aborts);

          sp_n_tx_done_.Set(sp_n_tx_done_.value_+1);
          num_try.fetch_add(coo->n_retry_);
          coo->sp_ev_done_.reset();
          coo->sp_ev_commit_.reset();
          free_coordinators_.push_back(coo);
          coo->_inuse_ = false;
        });
      }
      n_ceased_client_.Set(n_ceased_client_.value_+1);
    });
    poll_mgr_->add(dynamic_pointer_cast<Job>(sp_job));
  }
  poll_mgr_->add(dynamic_pointer_cast<Job>(std::make_shared<OneTimeJob>([this](){
    Log_info("wait for all virtual clients to stop issuing new requests.");
    n_ceased_client_.WaitUntilGreaterOrEqualThan(n_concurrent_);
    Log_info("wait for all outstanding requests to finish.");
    // TODO uncomment this, otherwise many requests are still outstanding there.
    sp_n_tx_done_.WaitUntilGreaterOrEqualThan(n_tx_issued_);
    // for debug purpose
//    Reactor::CreateSpEvent<NeverEvent>()->Wait(5*1000*1000);
    all_done_ = 1;
  })));

  while (all_done_ == 0) {
    Log_debug("wait for finish... n_ceased_cleints: %d,  "
              "n_issued: %d, n_done: %d, n_created_coordinator: %d",
              (int) n_ceased_client_.value_, (int) n_tx_issued_,
              (int) sp_n_tx_done_.value_, (int) created_coordinators_.size());
    sleep(1);
  }

  /*
  Log_info("Finish:\nTotal: %u, Commit: %u, Attempts: %u, Running for %u\n",
           num_txn.load(),
           success.load(),
           num_try.load(),
           Config::GetConfig()->get_duration());
  */
    Log_info("Finish:\nTotal: %u, Commit: %u, Attempts: %u, SSID_consistent: %u, Decided: %u, "
             "Validation_pass: %u, "
             "Cascading_aborts: %u, Rotxn_aborts: %u, Early_aborts: %lu, Running for %u\n",
             num_txn.load(),
             success.load(),
             num_try.load(),
             ssid_consistent.load(),
             decided.load(),
             // offset_valid.load(),
             validation_passed.load(),
             // single_shard.load(),
             // single_shard_write_only.load(),
             cascading_aborts.load(),
             rotxn_aborts.load(),
             early_aborts.load(),
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

/*
void ClientWorker::Work() {
  Log_debug("%s: %d", __FUNCTION__, this->cli_id_);
  txn_reg_ = std::make_shared<TxnRegistry>();
  verify(config_ != nullptr);
  Workload* workload = Workload::CreateWorkload(config_);
  workload->txn_reg_ = txn_reg_;
  workload->RegisterPrecedures();

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
    int n = n_concurrent_;
    auto sp_job = std::make_shared<OneTimeJob>([this] () {
      for (uint32_t n_tx = 0; n_tx < n_concurrent_; n_tx++) {
        auto coo = CreateCoordinator(n_tx);
        Log_debug("create coordinator %d", coo->coo_id_);
        this->DispatchRequest(coo);
      }
    });
    poll_mgr_->add(dynamic_pointer_cast<Job>(sp_job));
  } else {
    Log_info("open loop clients.");
    const std::chrono::nanoseconds wait_time
        ((int) (pow(10, 9) * 1.0 / (double) config_->client_rate_));
    double tps = 0;
    long txn_count = 0;
    auto start = std::chrono::steady_clock::now();
    std::chrono::nanoseconds elapsed;

    while (timer_->elapsed() < duration) {
      while (tps < config_->client_rate_ && timer_->elapsed() < duration) {
        auto coo = FindOrCreateCoordinator();
        if (coo != nullptr) {
          auto p_job = (Job*)new OneTimeJob([this, coo] () {
            this->DispatchRequest(coo);
          });
          shared_ptr<Job> sp_job(p_job);
          poll_mgr_->add(sp_job);
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
      tps = (double) txn_count / elapsed.count() * pow(10, 9);
    }
    Log_debug("exit client dispatch loop...");
  }

//  finish_mutex.lock();
  while (n_concurrent_ > 0) {
    Log_debug("wait for finish... %d", n_concurrent_);
    sleep(1);
//    finish_cond.wait(finish_mutex);
  }
//  finish_mutex.unlock();

  Log_info("Finish:\nTotal: %u, Commit: %u, Attempts: %u, SSID_consistent: %u, Decided: %u, Offset_valid: %u, "
           "Validation_pass: %u, Single_shard: %u, Single_shard_write_only: %u, "
           "Cascading_aborts: %u, Running for %u\n",
           num_txn.load(),
           success.load(),
           num_try.load(),
           ssid_consistent.load(),
           decided.load(),
           offset_valid.load(),
           validation_passed.load(),
           single_shard.load(),
           single_shard_write_only.load(),
           cascading_aborts.load(),
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
*/

void ClientWorker::AcceptForwardedRequest(TxRequest& request,
                                          TxReply* txn_reply,
                                          rrr::DeferredReply* defer) {
  const char* f = __FUNCTION__;

  // obtain free a coordinator first
  Coordinator* coo = nullptr;
  while (coo == nullptr) {
    coo = FindOrCreateCoordinator();
  }
  coo->forward_status_ = PROCESS_FORWARD_REQUEST;

  // run the task
  std::function<void()> task = [=]() {
    TxRequest req(request);
    req.callback_ = std::bind(&ClientWorker::ForwardRequestDone,
                              this,
                              coo,
                              txn_reply,
                              defer,
                              std::placeholders::_1);
    Log_debug("%s: running forwarded request at site %d", f, my_site_.id);
    coo->DoTxAsync(req);
  };
  task();
//  dispatch_pool_->run_async(task); // this causes bug
}

void ClientWorker::DispatchRequest(Coordinator* coo) {
  const char* f = __FUNCTION__;
  std::function<void()> task = [=]() {
    Log_debug("%s: %d", f, cli_id_);
    // TODO don't use pointer here.
    TxRequest *req = new TxRequest;
    {
      std::lock_guard<std::mutex> lock(this->request_gen_mutex);
      tx_generator_->GetTxRequest(req, coo->coo_id_);
    }
//     req.callback_ = std::bind(&ClientWorker::RequestDone,
//                               this,
//                               coo,
//                               std::placeholders::_1);
    req->callback_ = [coo, req] (TxReply&) {
//      verify(coo->sp_ev_commit_->status_ != Event::WAIT);
      coo->sp_ev_commit_->Set(1);
      auto& status = coo->sp_ev_done_->status_;
      verify(status == Event::WAIT || status == Event::INIT);
      coo->sp_ev_done_->Set(1);
      delete req;
    };
    coo->DoTxAsync(*req);
  };
  task();
//  dispatch_pool_->run_async(task); // this causes bug
}

/*
void ClientWorker::DispatchRequest(Coordinator* coo) {
  const char* f = __FUNCTION__;
  std::function<void()> task = [=]() {
    Log_debug("%s: %d", f, cli_id_);
    TxRequest req;
    {
      std::lock_guard<std::mutex> lock(this->request_gen_mutex);
      tx_generator_->GetTxRequest(&req, coo->coo_id_);
    }
    req.callback_ = std::bind(&ClientWorker::RequestDone,
                              this,
                              coo,
                              std::placeholders::_1);
    coo->DoTxAsync(req);
  };
  task();
//  dispatch_pool_->run_async(task); // this causes bug
}

*/

ClientWorker::ClientWorker(
    uint32_t id,
    Config::SiteInfo& site_info,
    Config* config,
    ClientControlServiceImpl* ccsi,
    PollMgr* poll_mgr) :
    id(id),
    my_site_(site_info),
    config_(config),
    cli_id_(site_info.id),
    benchmark(config->benchmark()),
    mode(config->get_mode()),
    duration(config->get_duration()),
    ccsi(ccsi),
    n_concurrent_(config->get_concurrent_txn()) {
  poll_mgr_ = poll_mgr == nullptr ? new PollMgr(1) : poll_mgr;
  frame_ = Frame::GetFrame(config->tx_proto_);
  tx_generator_ = frame_->CreateTxGenerator();
  config->get_all_site_addr(servers_);
  num_txn.store(0);
  success.store(0);
  num_try.store(0);
  ssid_consistent.store(0);
  decided.store(0);
  // offset_valid.store(0);
  validation_passed.store(0);
  cascading_aborts.store(0);
  rotxn_aborts.store(0);
        early_aborts.store(0);
  // single_shard.store(0);
  // single_shard_write_only.store(0);
  commo_ = frame_->CreateCommo(poll_mgr_);
  commo_->loc_id_ = my_site_.locale_id;
  forward_requests_to_leader_ =
      (config->replica_proto_ == MODE_MULTI_PAXOS && site_info.locale_id != 0) ? true
                                                                         : false;
  Log_debug("client %d created; forward %d",
            cli_id_,
            forward_requests_to_leader_);
}

} // namespace janus

