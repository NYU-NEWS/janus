#include "client_worker.h"
#include "frame.h"
#include "txn_chopper.h"
#include "coordinator.h"
#include "piece.h"
#include "benchmark_control_rpc.h"

namespace rococo {

ClientWorker::~ClientWorker() {
  if (txn_req_factory_) {
    delete txn_req_factory_;
  }
  for (auto c : coos_) {
    delete c;
  }
  coos_.clear();
}

void ClientWorker::RequestDone(Coordinator* coo, TxnReply &txn_reply) {
  verify(coo != nullptr);
  if (timer_->elapsed() < duration) {
    Log_debug("there is still time to issue another request. continue.");
    TxnRequest req;
    txn_req_factory_->get_txn_req(&req, cli_id_);
    req.callback_ = std::bind(&ClientWorker::RequestDone,
                              this,
                              coo,
                              std::placeholders::_1);
    coo->do_one(req);
  } else {
    Log_debug("client worker times up. stop.");
    finish_mutex.lock();
    n_concurrent_--;
    if (n_concurrent_ == 0)
      finish_cond.signal();
    finish_mutex.unlock();
  }
  if (txn_reply.res_ == SUCCESS)
    success++;
  num_txn++;
  num_try.fetch_add(txn_reply.n_try_);
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
  verify(n_concurrent_ > 0);
  auto commo = frame_->CreateCommo();
  commo->loc_id_ = my_site_.locale_id;
  for (uint32_t n_txn = 0; n_txn < n_concurrent_; n_txn++) {
    cooid_t coo_id = cli_id_;
    coo_id = (coo_id << 16) + n_txn;
    auto coo = frame_->CreateCoord(coo_id,
                                   config_,
                                   benchmark,
                                   ccsi,
                                   id,
                                   txn_reg_);
    coo->loc_id_ = my_site_.locale_id;
    coo->commo_ = commo;
    verify(coo->loc_id_ < 100);
    Log_debug("after create coo");

    TxnRequest req;
    txn_req_factory_->get_txn_req(&req, coo_id);
    req.callback_ = std::bind(&ClientWorker::RequestDone,
                              this,
                              coo,
                              std::placeholders::_1);
    coos_.push_back(coo);
    coo->do_one(req);
  }
  finish_mutex.lock();
  while (n_concurrent_ > 0) {
    finish_cond.wait(finish_mutex);
  }
  finish_mutex.unlock();
  Log_info("Finish:\nTotal: %u, Commit: %u, Attempts: %u, Running for %u\n",
           num_txn.load(),
           success.load(),
           num_try.load(),
           Config::GetConfig()->get_duration());

  if (ccsi) {
    Log_info("%s: wait_for_shutdown at client %d", __FUNCTION__, cli_id_);
    ccsi->wait_for_shutdown();
  }
  delete timer_;
  return;
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
  txn_req_factory_ = frame_->CreateTxnGenerator();
  config->get_all_site_addr(servers_);
  num_txn.store(0);
  success.store(0);
  num_try.store(0);
}

} // namespace rococo


