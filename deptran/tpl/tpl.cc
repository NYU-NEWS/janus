
#include "tpl.h"
#include "scheduler.h"

namespace rococo {

TPLDTxn::TPLDTxn(epoch_t epoch,
                 txnid_t tid,
                 Scheduler *sched) : DTxn(epoch, tid, sched) {

}

bool TPLDTxn::ReadColumn(mdb::Row *row,
                         mdb::column_id_t col_id,
                         Value *value,
                         int hint_flag) {
  if (locking_ && hint_flag != TXN_BYPASS) {
    locks_.push_back(mdb::column_lock_t(row, col_id, ALock::RLOCK));
  } else {
    auto ret = mdb_txn()->read_column(row, col_id, value);
    verify(ret == true);
  }
  return true;
}

bool TPLDTxn::ReadColumns(Row *row,
                          const std::vector<column_id_t> &col_ids,
                          std::vector<Value> *values,
                          int hint_flag) {
  if (locking_ && hint_flag != TXN_BYPASS) {
    for (auto col_id : col_ids) {
      locks_.push_back(mdb::column_lock_t(row, col_id, ALock::RLOCK));
    }
  } else {
    values->clear();
    auto ret = mdb_txn()->read_columns(row, col_ids, values);
    verify(ret == true);
  }
  return true;
}

bool TPLDTxn::WriteColumn(Row *row,
                          column_id_t col_id,
                          const Value &value,
                          int hint_flag) {
  if (locking_  && hint_flag != TXN_BYPASS) {
    locks_.push_back(mdb::column_lock_t(row, col_id, ALock::WLOCK));
  } else {
    auto ret = mdb_txn()->write_column(row, col_id, value);
    verify(ret == true);
  }
  return true;
}

bool TPLDTxn::WriteColumns(Row *row,
                           const std::vector<column_id_t> &col_ids,
                           const std::vector<Value> &values,
                           int hint_flag) {

  if (locking_  && hint_flag != TXN_BYPASS) {
    for (auto col_id : col_ids) {
      locks_.push_back(mdb::column_lock_t(row, col_id, ALock::WLOCK));
    }
  } else {
    auto ret = mdb_txn()->write_columns(row, col_ids, values);
    verify(ret == true);
  }
  return true;
}

bool TPLDTxn::InsertRow(Table *tbl, Row *row) {
  if (locking_) {
  } else {
    mdb_txn()->insert_row(tbl, row);
  }
  return true;
}

} // namespace rococo
