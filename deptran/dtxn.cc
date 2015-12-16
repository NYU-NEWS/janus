#include "all.h"
#include "frame.h"
#include "scheduler.h"

namespace rococo {

//void entry_t::touch(Vertex<TxnInfo> *tv, bool immediate ) {
//    int8_t edge_type = immediate ? EDGE_I : EDGE_D;
//    if (last_ != NULL) {
//        last_->to_[tv] |= edge_type;
//        tv->from_[last_] |= edge_type;
//    } else {
//        last_ = tv;
//    }
//}



DTxn::~DTxn() {

}

//mdb::ResultSet DTxn::query_in(Table *tbl,
//                              const mdb::SortedMultiKey &low,
//                              const mdb::SortedMultiKey &high,
//                              mdb::symbol_t order) {
//  verify(mdb_txn_ != nullptr);
//  return mdb_txn_->query_in(tbl, low, high, order);
//}

mdb::ResultSet DTxn::QueryIn(Table *tbl,
                             const mdb::MultiBlob &low,
                             const mdb::MultiBlob &high,
                             bool retrieve,
                             int64_t pid,
                             mdb::symbol_t order) {
  verify(mdb_txn_ != nullptr);
  return mdb_txn_->query_in(tbl, low, high, retrieve, pid, order);
}


//mdb::ResultSet DTxn::query(Table *tbl, const mdb::Value &kv) {
//  verify(mdb_txn_ != nullptr);
//  return mdb_txn_->query(tbl, kv);
//}

mdb::ResultSet DTxn::Query(Table *tbl,
                           const Value &kv,
                           bool retrieve,
                           int64_t pid) {
  verify(mdb_txn_ != nullptr);
  return mdb_txn_->query(tbl, kv, retrieve, pid);
}

//mdb::ResultSet DTxn::query(Table *tbl, const mdb::MultiBlob &mb) {
//  verify(mdb_txn_ != nullptr);
//  return mdb_txn_->query(tbl, mb);
//}

mdb::ResultSet DTxn::Query(mdb::Table *tbl,
                           const mdb::MultiBlob &mb,
                           bool retrieve,
                           int64_t pid) {
  verify(mdb_txn_ != nullptr);
  return mdb_txn_->query(tbl, mb, retrieve, pid);
}

bool DTxn::ReadColumn(mdb::Row *row,
                      mdb::column_id_t col_id,
                      Value *value,
                      bool unsafe) {
  verify(mdb_txn_ != nullptr);
  auto ret = mdb_txn_->read_column(row, col_id, value);
  verify(ret == true);
  return true;
}

bool DTxn::ReadColumns(Row *row,
                       const std::vector<column_id_t> &col_ids,
                       std::vector<Value> *values,
                       bool unsafe) {
  verify(mdb_txn_ != nullptr);
  auto ret = mdb_txn_->read_columns(row, col_ids, values);
  verify(ret == true);
  return true;
}

bool DTxn::WriteColumn(Row *row,
                       column_id_t col_id,
                       const Value &value,
                       bool unsafe) {
  verify(mdb_txn_ != nullptr);
  auto ret = mdb_txn_->write_column(row, col_id, value);
  verify(ret == true);
  return true;
}

bool DTxn::WriteColumns(Row *row,
                        const std::vector<column_id_t> &col_ids,
                        const std::vector<Value> &values,
                        bool unsafe) {
  verify(mdb_txn_ != nullptr);
  auto ret = mdb_txn_->write_columns(row, col_ids, values);
  verify(ret == true);
  return true;
}

//
//bool DTxn::ReadColumnUnsafe(mdb::Row *row,
//                            mdb::column_id_t col_id,
//                            Value *value) {
//  ReadColumn(row, col_id, value);
//}
//
//bool DTxn::ReadColumnsUnsafe(Row *row,
//                             const std::vector<column_id_t> &col_ids,
//                             std::vector<Value> *values) {
//  ReadColumns(row, col_ids, values);
//}
//
//bool DTxn::WriteColumnUnsafe(Row *row,
//                             column_id_t col_id,
//                             const Value &value) {
//  WriteColumn(row, col_id, value);
//}
//
//bool DTxn::WriteColumnsUnsafe(Row *row,
//                        const std::vector<column_id_t> &col_ids,
//                        const std::vector<Value> &values) {
//  WriteColumns(row, col_ids, values);
//}


bool DTxn::InsertRow(Table *tbl, Row *row) {
  verify(mdb_txn_ != nullptr);
  return mdb_txn_->insert_row(tbl, row);
}
//bool DTxn::remove_row(Table *tbl, Row *row) {
//  verify(mdb_txn_ != nullptr);
//  return mdb_txn_->remove_row(tbl, row);
//}

// TODO remove
mdb::Table *DTxn::GetTable(const std::string &tbl_name) const {
  return mgr_->get_table(tbl_name);
}

} // namespace rococo
