//
// Created by lamont on 1/11/16.
//

#include "MdccDTxn.h"
bool mdcc::MdccDTxn::ReadColumn(mdb::Row *row, mdb::column_id_t col_id, Value *value, int hint_flag) {
  return DTxn::ReadColumn(row, col_id, value, hint_flag);
}
bool mdcc::MdccDTxn::ReadColumns(mdb::Row *row,
                                 const vector<mdb::column_id_t> &col_ids,
                                 std::vector<Value> *values,
                                 int hint_flag) {
  return DTxn::ReadColumns(row, col_ids, values, hint_flag);
}
bool mdcc::MdccDTxn::WriteColumn(mdb::Row *row, mdb::column_id_t col_id, const Value &value, int hint_flag) {
  return DTxn::WriteColumn(row, col_id, value, hint_flag);
}
bool mdcc::MdccDTxn::WriteColumns(mdb::Row *row,
                                  const vector<mdb::column_id_t> &col_ids,
                                  const std::vector<Value> &values,
                                  int hint_flag) {
  return DTxn::WriteColumns(row, col_ids, values, hint_flag);
}
bool mdcc::MdccDTxn::InsertRow(mdb::Table *tbl, mdb::Row *row) {
  return DTxn::InsertRow(tbl, row);
}
mdb::Row *mdcc::MdccDTxn::Query(mdb::Table *tbl, const mdb::MultiBlob &mb, int64_t row_context_id) {
  return DTxn::Query(tbl, mb, row_context_id);
}
mdb::ResultSet mdcc::MdccDTxn::QueryIn(mdb::Table *tbl,
                                       const mdb::MultiBlob &low,
                                       const mdb::MultiBlob &high,
                                       mdb::symbol_t order,
                                       int rs_context_id) {
  return DTxn::QueryIn(tbl, low, high, order, rs_context_id);
}
mdb::Table *mdcc::MdccDTxn::GetTable(const std::string &tbl_name) const {
  return DTxn::GetTable(tbl_name);
}
