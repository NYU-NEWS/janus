#include "client_worker.h"
#include "frame.h"

namespace rococo {

ClientWorker::~ClientWorker() {
  if (txn_req_factory_) {
    delete txn_req_factory_;
  }
}

void ClientWorker::RequestDone(TxnReply &txn_reply) {
  verify(coo_ != nullptr);
  if (timer_->elapsed() < duration) {
    TxnRequest req;
    txn_req_factory_->get_txn_req(&req, coo_id);
    req.callback_ = std::bind(&ClientWorker::RequestDone, this,
                              std::placeholders::_1);
    coo_->do_one(req);
  } else {
    Log::debug("time up. stop.");
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
  verify(coo_ == nullptr);
  txn_reg_ = new TxnRegistry();
  Piece *piece = Piece::get_piece(benchmark);
  piece->txn_reg_ = txn_reg_;
  piece->reg_all();
  coo_ = Frame().CreateCoord(coo_id, servers, config, benchmark,
                             mode, ccsi, id, batch_start, txn_reg_);
  if (ccsi) ccsi->wait_for_start(id);

  timer_ = new Timer();
  timer_->start();
  verify(n_outstanding_ == 1);
  for (uint32_t n_txn = 0; n_txn < n_outstanding_; n_txn++) {
    TxnRequest req;
    txn_req_factory_->get_txn_req(&req, coo_id);
    req.callback_ = std::bind(&ClientWorker::RequestDone, this,
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

  delete coo_;
  if (ccsi) ccsi->wait_for_shutdown();
  delete timer_;
  return;
}

  ClientWorker::ClientWorker(uint32_t id, Config::SiteInfo &site_info, Config *config, ClientControlServiceImpl *ccsi) :
    id(id),
    my_site_(site_info),
    config(config),
    coo_id(site_info.id),
    benchmark(config->get_benchmark()),
    mode(config->get_mode()),
    batch_start(config->get_batch_start()),
    duration(config->get_duration()),
    ccsi(ccsi),
    n_outstanding_(config->get_concurrent_txn()),
    txn_req_factory_(Frame().CreateTxnGenerator()) {
    config->get_all_site_addr(servers);
    num_txn.store(0);
    success.store(0);
    num_try.store(0);
  }
} // namespace rococo


