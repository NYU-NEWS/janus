//
// Created by shuai on 11/25/15.
//

#include "../__dep__.h"
#include "../constants.h"
#include "../dtxn.h"
#include "deptran/procedure.h"
#include "../scheduler.h"
#include "../rcc/graph.h"
#include "../rcc/graph_marshaler.h"
#include "../marshal-value.h"
#include "../rcc_rpc.h"
#include "sched.h"
#include "exec.h"
#include "tpl.h"

namespace janus {

TPLSched::TPLSched() : ClassicSched() {
  mdb_txn_mgr_ = new mdb::TxnMgr2PL();
}

mdb::Txn* TPLSched::del_mdb_txn(const i64 tid) {
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

mdb::Txn* TPLSched::get_mdb_txn(const i64 tid) {
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


bool TPLSched::BeforeAccess(TxBox& tx_box, Row* row, int col_idx) {
  mdb::FineLockedRow* fl_row = (mdb::FineLockedRow*) row;
  ALock* lock = fl_row->get_alock(col_idx);
  TplTxBox& tpl_tx_box = dynamic_cast<TplTxBox&>(tx_box);
  uint64_t lock_req_id = lock->Lock(0, ALock::WLOCK, tx_box.tid_);
  if (lock_req_id > 0) {
    tpl_tx_box.locked_locks_.push_back(std::pair<ALock*, uint64_t>(lock, lock_req_id));
    return true;
  } else {
    return false;
  }
}

bool TPLSched::DoPrepare(txnid_t tx_id) {
  // do nothing here?
  TplTxBox* tx_box = (TplTxBox*)GetOrCreateDTxn(tx_id);
  verify(!tx_box->inuse);
  tx_box->inuse = true;
  if (tx_box->wounded_) {
    // lock wounded, do nothing will abort all the locks in the end.
  } else {
    for (auto& pair: tx_box->locked_locks_) {
      ALock* alock = pair.first;
      uint64_t lock_req_id = pair.second;
      alock->DisableWound(lock_req_id);
    }
  }

  tx_box->inuse = false;
  return !tx_box->wounded_;
}

void TPLSched::DoCommit(TxBox& tx_box) {
  TplTxBox& tpl_tx_box = dynamic_cast<TplTxBox&>(tx_box);
  for (auto& pair : tpl_tx_box.locked_locks_) {
    pair.first->abort(pair.second);
  }
  auto mdb_txn = RemoveMTxn(tx_box.tid_);
  verify(mdb_txn == tx_box.mdb_txn_);
  mdb_txn->commit();
  delete mdb_txn;
}

void TPLSched::DoAbort(TxBox& tx_box) {
  TplTxBox& tpl_tx_box = dynamic_cast<TplTxBox&>(tx_box);
  for (auto& pair : tpl_tx_box.locked_locks_) {
    pair.first->abort(pair.second);
  }
  auto mdb_txn = RemoveMTxn(tx_box.tid_);
  verify(mdb_txn == tx_box.mdb_txn_);
  mdb_txn->abort();
  delete mdb_txn;
}

} // namespace janus
