#include "__dep__.h"
#include "dtxn.h"
#include "frame.h"
#include "scheduler.h"

namespace rococo {

DTxn::~DTxn() {
}

mdb::Txn* DTxn::mdb_txn() {
  if (mdb_txn_) return mdb_txn_;
  mdb_txn_ = sched_->GetOrCreateMTxn(tid_);
  verify(mdb_txn_ != nullptr);
  return mdb_txn_;
}

mdb::ResultSet DTxn::QueryIn(Table *tbl,
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

mdb::Row* DTxn::Query(mdb::Table *tbl,
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

bool DTxn::ReadColumn(mdb::Row *row,
                      mdb::column_id_t col_id,
                      Value *value,
                      int hint_flag) {
  verify(mdb_txn() != nullptr);
  auto ret = mdb_txn()->read_column(row, col_id, value);
  verify(ret == true);
  return true;
}

bool DTxn::ReadColumns(Row *row,
                       const std::vector<column_id_t> &col_ids,
                       std::vector<Value> *values,
                       int hint_flag) {
  verify(mdb_txn() != nullptr);
  auto ret = mdb_txn()->read_columns(row, col_ids, values);
  verify(ret == true);
  return true;
}

bool DTxn::WriteColumn(Row *row,
                       column_id_t col_id,
                       const Value &value,
                       int hint_flag) {
  verify(mdb_txn() != nullptr);
  auto ret = mdb_txn()->write_column(row, col_id, value);
  verify(ret == true);
  return true;
}

bool DTxn::WriteColumns(Row *row,
                        const std::vector<column_id_t> &col_ids,
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

mdb::Row* DTxn::CreateRow(const mdb::Schema *schema,
                          const std::vector<mdb::Value> &row_data) {
  Row* r;
  switch (Config::config_s->cc_mode_) {
    case MODE_2PL:
      r = mdb::FineLockedRow::create(schema, row_data);
      break;
    case MODE_OCC:
    case MODE_NONE:
      r = mdb::VersionedRow::create(schema, row_data);
      break;
    default:
      verify(0);
  }
  return r;
}

bool DTxn::InsertRow(Table *tbl, Row *row) {
  verify(mdb_txn() != nullptr);
  return mdb_txn()->insert_row(tbl, row);
}

// TODO remove
mdb::Table *DTxn::GetTable(const std::string &tbl_name) const {
  return sched_->get_table(tbl_name);
}

} // namespace rococo
