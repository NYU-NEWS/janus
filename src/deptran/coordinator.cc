#include "marshal-value.h"
#include "coordinator.h"
#include "frame.h"
#include "constants.h"
#include "sharding.h"
#include "workload.h"
#include "benchmark_control_rpc.h"

/**
 * What shoud we do to change this to asynchronous?
 * 1. Fisrt we need to have a queue to hold all transaction requests.
 * 2. pop next request, send start request for each piece until there is no
 *available left.
 *          in the callback, send the next piece of start request.
 *          if responses to all start requests are collected.
 *              send the finish request
 *                  in callback, remove it from queue.
 *
 *
 */

namespace janus {
std::mutex Coordinator::_dbg_txid_lock_{};
std::unordered_set<txid_t> Coordinator::_dbg_txid_set_{};

Coordinator::Coordinator(uint32_t coo_id,
                         int32_t benchmark,
                         ClientControlServiceImpl *ccsi,
                         uint32_t thread_id) : coo_id_(coo_id),
                                               benchmark_(benchmark),
                                               ccsi_(ccsi),
                                               thread_id_(thread_id),
                                               mtx_() {
  uint64_t k = coo_id_;
  k <<= 32;
  k++;
  this->next_pie_id_.store(k);
  this->next_txn_id_.store(k);
  recorder_ = NULL;
  retry_wait_ = Config::GetConfig()->retry_wait();

  // TODO this would be slow.
  vector<string> addrs;
  Config::GetConfig()->get_all_site_addr(addrs);
  site_prepare_.resize(addrs.size(), 0);
  site_commit_.resize(addrs.size(), 0);
  site_abort_.resize(addrs.size(), 0);
  site_piece_.resize(addrs.size(), 0);
}

Coordinator::~Coordinator() {
//  for (int i = 0; i < site_prepare_.size(); i++) {
//    Log_debug("Coo: %u, Site: %d, accept: %d, "
//                 "prepare: %d, commit: %d, abort: %d",
//             coo_id_, i, site_piece_[i], site_prepare_[i],
//             site_commit_[i], site_abort_[i]);
//  }

  if (recorder_) delete recorder_;
#ifdef TXN_STAT

  for (auto& it : txn_stats_) {
        Log::info("TXN: %d", it.first);
        it.second.output();
      }
#endif /* ifdef TXN_STAT */

  // debug;
  mtx_.lock();
  mtx_.unlock();
// TODO (shuai) destroy all the rpc clients and proxies.
}
} // namespace janus
