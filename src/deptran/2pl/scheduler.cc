//
// Created by shuai on 11/25/15.
//

#include "../__dep__.h"
#include "../constants.h"
#include "../tx.h"
#include "../scheduler.h"
#include "scheduler.h"
#include "tx.h"

namespace janus {

Scheduler2pl::Scheduler2pl() : SchedulerClassic() {
  mdb_txn_mgr_ = make_shared<mdb::TxnMgr2PL>();
}

mdb::Txn* Scheduler2pl::del_mdb_txn(const i64 tid) {
  mdb::Txn *txn = NULL;
  auto it = mdb_txns_.find(tid);
  if (it == mdb_txns_.end()) {
    verify(0);
  } else {
    txn = it->second;
  }
  mdb_txns_.erase(it);
  return txn;
}

mdb::Txn* Scheduler2pl::get_mdb_txn(const i64 tid) {
  mdb::Txn *txn = nullptr;
  auto it = mdb_txns_.find(tid);
  if (it == mdb_txns_.end()) {
    txn = mdb_txn_mgr_->start(tid);
    //XXX using occ lazy mode: increment version at commit time
    auto ret = mdb_txns_.insert(std::pair<i64, mdb::Txn *>(tid, txn));
    verify(ret.second);
  } else {
    txn = it->second;
  }

  verify(mdb_txn_mgr_->rtti() == mdb::symbol_t::TXN_2PL);
  verify(txn->rtti() == mdb::symbol_t::TXN_2PL);
  verify(txn != nullptr);
  return txn;
}


bool Scheduler2pl::Guard(Tx &tx_box, Row *row, int col_idx, bool write) {
  mdb::FineLockedRow* fl_row = (mdb::FineLockedRow*) row;
  ALock* lock = fl_row->get_alock(col_idx);
  auto sp_tx = dynamic_pointer_cast<Tx2pl>(tx_box.shared_from_this());
  uint64_t lock_req_id = lock->Lock(0, ALock::WLOCK, tx_box.tid_, [sp_tx]()->int{
    if (sp_tx->woundable_) {
      sp_tx->wounded_ = true;
      return 0;
    } else {
      return 1;
    }
  });
//  verify(!sp_tx->aborted_);
  verify(!sp_tx->committed_);
  if (lock_req_id > 0) {
    sp_tx->locked_locks_.emplace_back(lock, lock_req_id);
    return true;
  } else {
    return false;
  }
}

bool Scheduler2pl::DoPrepare(txnid_t tx_id) {
  // do nothing here?
  auto tx_box = dynamic_pointer_cast<Tx2pl>(GetOrCreateTx(tx_id));
  verify(!tx_box->inuse);
  tx_box->inuse = true;
  bool ret = true;
  if (tx_box->wounded_) {
    return false;
  }
  tx_box->woundable_ = false;
  tx_box->inuse = false;
  return ret;
}

void Scheduler2pl::DoCommit(Tx& tx_box) {
  Tx2pl& tpl_tx_box = dynamic_cast<Tx2pl&>(tx_box);
  for (auto& pair : tpl_tx_box.locked_locks_) {
    pair.first->abort(pair.second);
  }
  tpl_tx_box.committed_ = true;
  auto mdb_txn = RemoveMTxn(tx_box.tid_);
  verify(mdb_txn == tx_box.mdb_txn_);
  mdb_txn->commit();
  auto t = dynamic_pointer_cast<Tx2pl>(GetOrCreateTx(tx_box.tid_));
  tx_box.mdb_txn_ = nullptr;
  delete mdb_txn;
}

void Scheduler2pl::DoAbort(Tx& tx_box) {
  Tx2pl& tpl_tx_box = dynamic_cast<Tx2pl&>(tx_box);
  tpl_tx_box.aborted_ = true;
  for (auto& pair : tpl_tx_box.locked_locks_) {
    pair.first->abort(pair.second);
  }
  auto mdb_txn = RemoveMTxn(tx_box.tid_);
  verify(mdb_txn == tx_box.mdb_txn_);
  mdb_txn->abort();
  delete mdb_txn;
}

} // namespace janus
