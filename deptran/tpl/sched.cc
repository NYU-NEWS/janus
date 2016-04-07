//
// Created by shuai on 11/25/15.
//

#include "../__dep__.h"
#include "../constants.h"
#include "../dtxn.h"
#include "../scheduler.h"
#include "../rcc/graph.h"
#include "../rcc/graph_marshaler.h"
#include "../marshal-value.h"
#include "../rcc_rpc.h"
#include "sched.h"
#include "exec.h"
#include "tpl.h"

namespace rococo {

TPLSched::TPLSched() : ClassicSched() {
  mdb_txn_mgr_ = new mdb::TxnMgr2PL();
}

mdb::Txn* TPLSched::del_mdb_txn(const i64 tid) {
  mdb::Txn *txn = NULL;
  std::map<i64, mdb::Txn *>::iterator it = mdb_txns_.find(tid);
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
    //verify(IS_MODE_2PL);2
    txn = mdb_txn_mgr_->start(tid);
    //XXX using occ lazy mode: increment version at commit time
    std::pair<std::map<i64, mdb::Txn *>::iterator, bool> ret
        = mdb_txns_.insert(std::pair<i64, mdb::Txn *>(tid, txn));
    verify(ret.second);
  } else {
    txn = it->second;
  }

  verify(mdb_txn_mgr_->rtti() == mdb::symbol_t::TXN_2PL);
  verify(txn->rtti() == mdb::symbol_t::TXN_2PL);
  verify(txn != nullptr);
  return txn;
}

//mdb::Txn* TPLSched::get_mdb_txn(const RequestHeader &header) {
//  mdb::Txn *txn = nullptr;
//  txn = get_mdb_txn(header.tid);
//  return txn;
//}


} // namespace rococo
