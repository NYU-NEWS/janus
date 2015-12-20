#include "all.h"
#include "scheduler.h"

namespace rococo {

TPLDTxn::TPLDTxn(i64 tid, Scheduler *mgr) : DTxn(tid, mgr) {
  verify(mdb_txn_ == nullptr);
  mdb_txn_ = mgr->GetOrCreateMTxn(tid_);
  verify(mdb_txn_ != nullptr);
}


bool TPLDTxn::ReadColumn(mdb::Row *row,
                         mdb::column_id_t col_id,
                         Value *value,
                         bool bypass,
                         bool deferred) {
  if (locking_ && !bypass) {
//    verify(0);
    locks_.push_back(mdb::column_lock_t(row, col_id, ALock::RLOCK));
  } else {
    verify(mdb_txn_ != nullptr);
    auto ret = mdb_txn_->read_column(row, col_id, value);
    verify(ret == true);
  }
  return true;
}

bool TPLDTxn::ReadColumns(Row *row,
                          const std::vector<column_id_t> &col_ids,
                          std::vector<Value> *values,
                          bool bypass,
                          bool deferred) {
  if (locking_ && !bypass) {
//    verify(0);
    for (auto col_id : col_ids) {
      locks_.push_back(mdb::column_lock_t(row, col_id, ALock::RLOCK));
    }
  } else {
    values->clear();
    verify(mdb_txn_ != nullptr);
    auto ret = mdb_txn_->read_columns(row, col_ids, values);
    verify(ret == true);
  }
  return true;
}

bool TPLDTxn::WriteColumn(Row *row,
                          column_id_t col_id,
                          const Value &value,
                          bool bypass,
                          bool deferred) {
  if (locking_  && !bypass) {
//    verify(0);
    locks_.push_back(mdb::column_lock_t(row, col_id, ALock::WLOCK));
  } else {
    verify(mdb_txn_ != nullptr);
    auto ret = mdb_txn_->write_column(row, col_id, value);
    verify(ret == true);
  }
  return true;
}

bool TPLDTxn::WriteColumns(Row *row,
                           const std::vector<column_id_t> &col_ids,
                           const std::vector<Value> &values,
                           bool bypass,
                           bool deferred) {

  if (locking_  && !bypass) {
//    verify(0);
    for (auto col_id : col_ids) {
      locks_.push_back(mdb::column_lock_t(row, col_id, ALock::WLOCK));
    }
  } else {
    verify(mdb_txn_ != nullptr);
    auto ret = mdb_txn_->write_columns(row, col_ids, values);
    verify(ret == true);
  }
  return true;
}

bool TPLDTxn::InsertRow(Table *tbl, Row *row) {
  if (locking_) {

  } else {
    verify(mdb_txn_ != nullptr);
    mdb_txn_->insert_row(tbl, row);
  }
  return true;
}

} // namespace rococo
