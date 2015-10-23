#include "client_worker.h"

namespace rococo {

ClientWorker::ClientWorker() {
  txn_req_factory_ = new TxnRequestFactory(Config::GetConfig()->sharding_);
}

void ClientWorker::callback2(TxnReply &txn_reply) {
  TIMER_IF_NOT_END {
    TxnRequest req;
    txn_req_factory_->get_txn_req(&req, coo_id);
    req.callback_ = std::bind(&ClientWorker::callback2, this,
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
  Coordinator *coo = GetCoord();
  if (ccsi) ccsi->wait_for_start(id);

  TIMER_SET(duration);
  verify(n_outstanding_ == 1);
  for (uint32_t n_txn = 0; n_txn < n_outstanding_; n_txn++) {
    TxnRequest req;
    txn_req_factory_->get_txn_req(&req, coo_id);
    req.callback_ = std::bind(&ClientWorker::callback2, this,
                              std::placeholders::_1);
    coo->do_one(req);
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

  delete coo;
  if (ccsi) ccsi->wait_for_shutdown();
  return;
}


} // namespace rococo


