

//
// Created by shuai on 11/25/15.
//

#include "../__dep__.h"
#include "../constants.h"
#include "../dtxn.h"
#include "../scheduler.h"
#include "../rcc/graph.h"
#include "../txn_chopper.h"
#include "../rcc/graph_marshaler.h"
#include "../marshal-value.h"
#include "../rcc_rpc.h"
#include "sched.h"

namespace rococo {

OCCSched::OCCSched() : ClassicSched() {
  mdb_txn_mgr_ = new mdb::TxnMgrOCC();
}

mdb::Txn* OCCSched::get_mdb_txn(const i64 tid) {
  mdb::Txn *txn = nullptr;
  auto it = mdb_txns_.find(tid);
  if (it == mdb_txns_.end()) {
    //verify(IS_MODE_2PL);
    txn = mdb_txn_mgr_->start(tid);
    //XXX using occ lazy mode: increment version at commit time
    ((mdb::TxnOCC *) txn)->set_policy(mdb::OCC_LAZY);
    auto ret = mdb_txns_.insert(std::pair<i64, mdb::Txn *>(tid, txn));
    verify(ret.second);
  } else {
    txn = it->second;
  }
  verify(mdb_txn_mgr_->rtti() == mdb::symbol_t::TXN_OCC);
  verify(txn->rtti() == mdb::symbol_t::TXN_OCC);
  verify(txn != nullptr);
  return txn;
}

} // namespace rococo
