#include "__dep__.h"
#include "dtxn.h"
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

mdb::Txn* DTxn::mdb_txn() {
  if (mdb_txn_) return mdb_txn_;
  mdb_txn_ = sched_->GetOrCreateMTxn(tid_);
  verify(mdb_txn_ != nullptr);
  return mdb_txn_;
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

//mdb::ResultSet DTxn::query(Table *tbl, const mdb::Value &kv) {
//  verify(mdb_txn_ != nullptr);
//  return mdb_txn_->query(tbl, kv);
//}

//mdb::ResultSet DTxn::Query(Table *tbl,
//                           const Value &kv,
//                           bool retrieve,
//                           int64_t pid) {
//  verify(mdb_txn_ != nullptr);
//  return mdb_txn_->query(tbl, kv, retrieve, pid);
//}

//mdb::ResultSet DTxn::query(Table *tbl, const mdb::MultiBlob &mb) {
//  verify(mdb_txn_ != nullptr);
//  return mdb_txn_->query(tbl, mb);
//}

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
//    verify(rs.has_next());
      ret_row = rs.next();
      row_map[row_context_id] = ret_row;
    }
  } else {
    ret_row = mdb_txn_->query(tbl, mb).next();
//  ret_row = mdb_txn_->query(tbl, mb, retrieve, pid).next();
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
//  auto ret = mdb_txn()->write_columns(row, col_ids, values);
  verify(col_ids.size() == values.size());
  for (int i = 0; i < col_ids.size(); i++) {
    auto ret = WriteColumn(row, col_ids[i], values[i], hint_flag);
    verify(ret == true);
  }
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
  verify(mdb_txn() != nullptr);
  return mdb_txn()->insert_row(tbl, row);
}
//bool DTxn::remove_row(Table *tbl, Row *row) {
//  verify(mdb_txn_ != nullptr);
//  return mdb_txn_->remove_row(tbl, row);
//}

// TODO remove
mdb::Table *DTxn::GetTable(const std::string &tbl_name) const {
  return sched_->get_table(tbl_name);
}

} // namespace rococo
