#include "__dep__.h"
#include "tx.h"
#include "frame.h"
#include "scheduler.h"

namespace janus {

Tx::~Tx() {
}

mdb::Txn* Tx::mdb_txn() {
  if (mdb_txn_) return mdb_txn_;
  mdb_txn_ = sched_->GetOrCreateMTxn(tid_);
  verify(mdb_txn_ != nullptr);
  return mdb_txn_;
}

mdb::ResultSet Tx::QueryIn(Table *tbl,
                             const mdb::MultiBlob &low,
                             const mdb::MultiBlob &high,
                             mdb::symbol_t order,
                             int rs_context_id) {
  verify(mdb_txn() != nullptr);
  auto &rs_map = context_rs_;
  if (rs_context_id > 0) {
    auto it = rs_map.find(rs_context_id);
    if (it != rs_map.end()) {
      mdb::ResultSet ret_rs = it->second;
      ret_rs.reset();
      return ret_rs;
    } else {
      mdb::ResultSet ret_rs = mdb_txn()->query_in(tbl, low, high, order);
      rs_map.insert(std::make_pair(rs_context_id, ret_rs));
      return ret_rs;
    }
  } else {
    verify(0);
  }
}

mdb::Row* Tx::Query(mdb::Table *tbl,
                      vector<Value>& primary_keys,
                      int64_t row_context_id) {
  mdb::MultiBlob mb(primary_keys.size());
  for (int i = 0; i < primary_keys.size(); i++) {
    mb[i] = primary_keys[i].get_blob();
  }
  auto r = Query(tbl, mb, row_context_id);
  return r;
}

mdb::Row* Tx::Query(mdb::Table *tbl,
                      const mdb::MultiBlob &mb,
                      int64_t row_context_id) {
  verify(mdb_txn() != nullptr);
  mdb::Row* ret_row;
  auto &row_map = context_row_;
  if (row_context_id > 0) {
    auto it = row_map.find(row_context_id);
    if (it != row_map.end()) {
      ret_row = it->second;
    } else {
      auto rs = mdb_txn()->query(tbl, mb);
      verify(rs.has_next());
      ret_row = rs.next();
      row_map[row_context_id] = ret_row;
    }
  } else {
    ret_row = mdb_txn_->query(tbl, mb).next();
  }
  return ret_row;
}

bool Tx::ReadColumn(mdb::Row *row,
                      mdb::colid_t col_id,
                      Value *value,
                      int hint_flag) {
  auto mtx = mdb_txn();
  verify(mtx);
  auto x = row->ref_count();
  verify(x > 0);
  verify(col_id >= 0);
  int y = value->get_kind();
  verify(y >= 0);
  auto z = mtx->rtti();
  verify(z>=0);
  auto ret = mtx->read_column(row, col_id, value);
  verify(ret == true);
  return true;
}

bool Tx::ReadColumns(Row *row,
                       const std::vector<colid_t> &col_ids,
                       std::vector<Value> *values,
                       int hint_flag) {
  verify(mdb_txn() != nullptr);
  auto ret = mdb_txn()->read_columns(row, col_ids, values);
  verify(ret == true);
  return true;
}

bool Tx::WriteColumn(Row *row,
                       colid_t col_id,
                       const Value &value,
                       int hint_flag) {
  verify(mdb_txn() != nullptr);
  auto ret = mdb_txn()->write_column(row, col_id, value);
  verify(ret == true);
  return true;
}

bool Tx::WriteColumns(Row *row,
                        const std::vector<colid_t> &col_ids,
                        const std::vector<Value> &values,
                        int hint_flag) {
  verify(mdb_txn() != nullptr);
  verify(col_ids.size() == values.size());
  for (int i = 0; i < col_ids.size(); i++) {
    auto ret = WriteColumn(row, col_ids[i], values[i], hint_flag);
    verify(ret == true);
  }
  return true;
}

mdb::Row* Tx::CreateRow(const mdb::Schema *schema,
                          const std::vector<mdb::Value> &row_data) {
  Row* r;
  switch (Config::config_s->tx_proto_) {
    case MODE_2PL:
      r = mdb::FineLockedRow::create(schema, row_data);
      break;
    case MODE_OCC:
    case MODE_NONE:
    default:
      r = mdb::VersionedRow::create(schema, row_data);
      break;
  }
  return r;
}

bool Tx::InsertRow(Table *tbl, Row *row) {
  verify(mdb_txn() != nullptr);
  return mdb_txn()->insert_row(tbl, row);
}

// TODO remove
mdb::Table *Tx::GetTable(const std::string &tbl_name) const {
  return sched_->get_table(tbl_name);
}

} // namespace janus
