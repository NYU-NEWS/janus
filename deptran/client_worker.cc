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
  verify(coo_);
  delete coo_;
  coo_ = nullptr;
}

void ClientWorker::RequestDone(TxnReply &txn_reply) {
  verify(coo_ != nullptr);
  if (timer_->elapsed() < duration) {
    Log_debug("there is still time to issue another request. continue.");
    TxnRequest req;
    txn_req_factory_->get_txn_req(&req, coo_id);
    req.callback_ = std::bind(&ClientWorker::RequestDone,
                              this,
                              std::placeholders::_1);
    coo_->do_one(req);
  } else {
    Log_debug("client worker times up. stop.");
    finish_mutex.lock();
    n_outstanding_--;
    if (n_outstanding_ == 0)
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
  verify(coo_ == nullptr);
  txn_reg_ = new TxnRegistry();
  Piece *piece = Piece::get_piece(benchmark);
  piece->txn_reg_ = txn_reg_;
  piece->reg_all();
  coo_ = frame_->CreateCoord(coo_id,
                             config,
                             benchmark,
                             ccsi,
                             id,
                             txn_reg_);
  Log_debug("after create coo");
  if (ccsi) ccsi->wait_for_start(id);
  Log_debug("after wait for start");

  timer_ = new Timer();
  timer_->start();
  verify(n_outstanding_ == 1);
  for (uint32_t n_txn = 0; n_txn < n_outstanding_; n_txn++) {
    TxnRequest req;
    txn_req_factory_->get_txn_req(&req, coo_id);
    req.callback_ = std::bind(&ClientWorker::RequestDone,
                              this,
                              std::placeholders::_1);
    coo_->do_one(req);
  }
  finish_mutex.lock();
  while (n_outstanding_ > 0) {
    finish_cond.wait(finish_mutex);
  }
  finish_mutex.unlock();
  Log_info("Finish:\nTotal: %u, Commit: %u, Attempts: %u, Running for %u\n",
           num_txn.load(),
           success.load(),
           num_try.load(),
           Config::GetConfig()->get_duration());

  if (ccsi) {
    Log_info("%s: wait_for_shutdown at client %d", __FUNCTION__, this->coo_id);
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
      config(config),
      coo_id(site_info.id),
      benchmark(config->get_benchmark()),
      mode(config->get_mode()),
      batch_start(config->get_batch_start()),
      duration(config->get_duration()),
      ccsi(ccsi),
      n_outstanding_(config->get_concurrent_txn()) {
  frame_ = Frame::GetFrame(config->cc_mode_);
  txn_req_factory_ = frame_->CreateTxnGenerator();
  config->get_all_site_addr(servers_);
  num_txn.store(0);
  success.store(0);
  num_try.store(0);
}

} // namespace rococo


