#include "all.h"
#include "frame.h"
#include "dtxn_mgr.h"

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

mdb::ResultSet DTxn::query_in(Table *tbl,
                              const mdb::SortedMultiKey &low,
                              const mdb::SortedMultiKey &high,
                              mdb::symbol_t order) {
  verify(mdb_txn_ != nullptr);
  return mdb_txn_->query_in(tbl, low, high, order);
}

mdb::ResultSet DTxn::query_in(Table *tbl,
                              const mdb::MultiBlob &low,
                              const mdb::MultiBlob &high,
                              bool retrieve,
                              int64_t pid,
                              mdb::symbol_t order) {
  verify(mdb_txn_ != nullptr);
  return mdb_txn_->query_in(tbl, low, high, retrieve, pid, order);
}


mdb::ResultSet DTxn::query(Table *tbl, const mdb::Value &kv) {
  verify(mdb_txn_ != nullptr);
  return mdb_txn_->query(tbl, kv);
}

mdb::ResultSet DTxn::query(Table *tbl, const Value &kv, bool retrieve, int64_t pid) {
  verify(mdb_txn_ != nullptr);
  return mdb_txn_->query(tbl, kv, retrieve, pid);
}

mdb::ResultSet DTxn::query(Table *tbl, const mdb::MultiBlob &mb) {
  verify(mdb_txn_ != nullptr);
  return mdb_txn_->query(tbl, mb);
}

mdb::ResultSet DTxn::query(
    mdb::Table *tbl,
    const mdb::MultiBlob &mb,
    bool retrieve,
    int64_t pid) {
  verify(mdb_txn_ != nullptr);
  return mdb_txn_->query(tbl, mb, retrieve, pid);
}

bool DTxn::read_column(
    mdb::Row *row,
    mdb::column_id_t col_id,
    Value *value) {
  verify(mdb_txn_ != nullptr);
  auto ret = mdb_txn_->read_column(row, col_id, value);
  verify(ret == true);
  return true;
}

bool DTxn::read_columns(
    Row *row,
    const std::vector<column_id_t> &col_ids,
    std::vector<Value> *values) {
  verify(mdb_txn_ != nullptr);
  auto ret = mdb_txn_->read_columns(row, col_ids, values);
  verify(ret == true);
  return true;
}

bool DTxn::write_column(Row *row, column_id_t col_id, const Value &value) {
  verify(mdb_txn_ != nullptr);
  auto ret = mdb_txn_->write_column(row, col_id, value);
  verify(ret == true);
  return true;
}

bool DTxn::write_columns(
    Row *row,
    const std::vector<column_id_t> &col_ids,
    const std::vector<Value> &values) {
  verify(mdb_txn_ != nullptr);
  auto ret = mdb_txn_->write_columns(row, col_ids, values);
  verify(ret == true);
  return true;
}


bool DTxn::insert_row(Table *tbl, Row *row) {
  verify(mdb_txn_ != nullptr);
  return mdb_txn_->insert_row(tbl, row);
}
bool DTxn::remove_row(Table *tbl, Row *row) {
  verify(mdb_txn_ != nullptr);
  return mdb_txn_->remove_row(tbl, row);
}

mdb::Table *DTxn::get_table(const std::string &tbl_name) const {
  return mgr_->get_table(tbl_name);
}

} // namespace rococo
