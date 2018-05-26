
#include "tx.h"

namespace janus {


bool TxOcc::ReadColumn(mdb::Row *row,
                      mdb::colid_t col_id,
                      Value *value,
                      int hint_flag) {
  verify(mdb_txn() != nullptr);
  auto ret = mdb_txn()->read_column(row, col_id, value);
  verify(ret == true);
  return true;
}

bool TxOcc::ReadColumns(Row *row,
                          const std::vector<colid_t> &col_ids,
                          std::vector<Value> *values,
                          int hint_flag) {
  verify(mdb_txn() != nullptr);
  auto ret = mdb_txn()->read_columns(row, col_ids, values);
  verify(ret == true);
  return true;
}

bool TxOcc::WriteColumn(Row *row,
                          colid_t col_id,
                          const Value &value,
                          int hint_flag) {
  verify(mdb_txn() != nullptr);
  auto ret = mdb_txn()->write_column(row, col_id, value);
  verify(ret == true);
  return true;
}

bool TxOcc::WriteColumns(Row *row,
                           const std::vector<colid_t> &col_ids,
                           const std::vector<Value> &values,
                           int hint_flag) {
  verify(mdb_txn() != nullptr);
  auto ret = mdb_txn()->write_columns(row, col_ids, values);
  verify(ret == true);
  return true;
}


bool TxOcc::InsertRow(Table *tbl, Row *row) {
  verify(mdb_txn() != nullptr);
  return mdb_txn()->insert_row(tbl, row);
}

} // namespace janus;
