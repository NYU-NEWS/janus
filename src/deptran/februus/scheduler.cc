#include "tx.h"
#include "scheduler.h"

namespace janus {

bool SchedulerFebruus::Guard(Tx &tx,
                             Row *row,
                             int col_id,
                             bool write) {

  auto row_ver = dynamic_cast<mdb::VersionedRow *>(row);
  auto &ver = row_ver->ver_[col_id];
  auto &tx_feb = dynamic_cast<TxFebruus &>(tx);
  tx_feb.timestamp_ = tx_feb.timestamp_ > ver ?
                           tx_feb.timestamp_ : ver;
  return true;
}

int SchedulerFebruus::OnPreAccept(const txid_t tx_id,
                                  uint64_t& ret_timestamp) {
  Log_debug("pre-accept tx id: %" PRIx64, tx_id);
  auto sp_tx = dynamic_pointer_cast<TxFebruus>(GetOrCreateTx(tx_id));
  sp_tx->fully_dispatched_->Wait();
  if (sp_tx->max_seen_ballot_ > 0) {
    return REJECT;
  }
  ret_timestamp = sp_tx->timestamp_;
  UpdateQueue(sp_tx);
  return SUCCESS;
}

int SchedulerFebruus::OnAccept(const txid_t tx_id,
                               uint64_t timestamp,
                               ballot_t ballot) {
  Log_debug("accept tx id: %" PRIx64, tx_id);
  auto sp_tx = dynamic_pointer_cast<TxFebruus>(GetOrCreateTx(tx_id));
  sp_tx->fully_dispatched_->Wait();
  if (sp_tx->max_seen_ballot_ > ballot) {
    return REJECT;
  }
  sp_tx->max_seen_ballot_ = ballot;
  sp_tx->timestamp_ = timestamp;
  UpdateQueue(sp_tx);
  return SUCCESS;
}

int SchedulerFebruus::OnCommit(const txid_t tx_id,
                               uint64_t timestamp) {
  auto sp_tx = dynamic_pointer_cast<TxFebruus>(GetOrCreateTx(tx_id));
  Log_debug("commit tx id: %" PRIx64, tx_id);
  sp_tx->fully_dispatched_->Wait();
  sp_tx->max_seen_ballot_ = -1;
  sp_tx->max_accepted_ballot_ = -1;
  sp_tx->timestamp_ = timestamp;
  sp_tx->committed_ = true;
  UpdateQueue(sp_tx);
  return SUCCESS;
}

void SchedulerFebruus::UpdateQueue(shared_ptr<TxFebruus> sp_tx) {
  // remove it first to be safe.
  auto it = queue_.begin();
  while (it != queue_.end()) {
    if ((*it)->tid_ == sp_tx->tid_) {
      it = queue_.erase(it);
      break;
    }
    it++;
  }

  // insert again to the right place.
  it = queue_.begin();
  while (it != queue_.end()) {
    shared_ptr<TxFebruus> sp_tx_max = *it;
    if ((*it)->timestamp_ > sp_tx->timestamp_) {
      break; // TODO handle the case where txs have the same timestamp
    }
    it++;
  }
  queue_.insert(it, sp_tx);

  // execute
  it = queue_.begin();
  while (it != queue_.end() && (*it) -> committed_) {
    (*it)->ev_execute_ready_->Set(1);
    it = queue_.erase(it);
  }
}

} // namespace janus
