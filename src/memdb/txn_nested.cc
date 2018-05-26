#include <limits>

#include "row.h"
#include "table.h"
#include "txn.h"
#include "txn_2pl.h"
#include "txn_occ.h"
#include "txn_unsafe.h"
#include "txn_nested.h"

using namespace std;

namespace mdb {

void TxnNested::abort() {
  verify(outcome_ == symbol_t::NONE);
  outcome_ = symbol_t::TXN_ABORT;
}

bool TxnNested::commit() {
  verify(outcome_ == symbol_t::NONE);
  for (auto &it : inserts_) {
    base_->insert_row(it.table, it.row);
  }
  for (auto &it : updates_) {
    Row *row = it.first;
    colid_t column_id = it.second.first;
    Value &value = it.second.second;
    base_->write_column(row, column_id, value);
  }
  for (auto &it : removes_) {
    base_->remove_row(it.table, it.row);
  }
  outcome_ = symbol_t::TXN_COMMIT;
  return true;
}

bool TxnNested::read_column(Row *row, colid_t col_id, Value *value) {
  assert(debug_check_row_valid(row));
  verify(outcome_ == symbol_t::NONE);

  // note that we cannot check `row->get_table() == nullptr` and assume the row
  // could be directly accessed. this checking is ok for top level transaction,
  // but for nested transaction, we need to explicitly check if `row in inserts_`
  if (row_inserts_.find(row) != row_inserts_.end()) {
    // row not inserted into table, just read from staging area
    *value = row->get_column(col_id);
    return true;
  }

  auto eq_range = updates_.equal_range(row);
  for (auto it = eq_range.first; it != eq_range.second; ++it) {
    if (it->second.first == col_id) {
      *value = it->second.second;
      return true;
    }
  }

  // reading from base transaction
  return base_->read_column(row, col_id, value);
}

bool TxnNested::write_column(Row *row, colid_t col_id, const Value &value) {
  assert(debug_check_row_valid(row));
  verify(outcome_ == symbol_t::NONE);

  // note that we cannot check `row->get_table() == nullptr` and assume the row
  // could be directly accessed. this checking is ok for top level transaction,
  // but for nested transaction, we need to explicitly check if `row in inserts_`
  if (row_inserts_.find(row) != row_inserts_.end()) {
    // row not inserted into table, just write to staging area
    row->update(col_id, value);
    return true;
  }

  auto eq_range = updates_.equal_range(row);
  for (auto it = eq_range.first; it != eq_range.second; ++it) {
    if (it->second.first == col_id) {
      it->second.second = value;
      return true;
    }
  }

  // cache updates
  insert_into_map(updates_, row, make_pair(col_id, value));

  return true;
}

bool TxnNested::insert_row(Table *tbl, Row *row) {
  verify(outcome_ == symbol_t::NONE);
  verify(row->get_table() == nullptr);
  inserts_.insert(table_row_pair(tbl, row));
  row_inserts_.insert(row);
  removes_.erase(table_row_pair(tbl, row));
  return true;
}

bool TxnNested::remove_row(Table *tbl, Row *row) {
  assert(debug_check_row_valid(row));
  verify(outcome_ == symbol_t::NONE);

  // we need to sweep inserts_ to find the Row with exact pointer match
  auto it_pair = inserts_.equal_range(table_row_pair(tbl, row));
  auto it = it_pair.first;
  while (it != it_pair.second) {
    if (it->row == row) {
      break;
    }
    ++it;
  }

  if (it == it_pair.second) {
    removes_.insert(table_row_pair(tbl, row));
  } else {
    it->row->release();
    inserts_.erase(it);
    row_inserts_.erase(row);
  }
  updates_.erase(row);

  return true;
}


ResultSet TxnNested::query(Table *tbl, const MultiBlob &mb) {
  KeyOnlySearchRow key_search_row(tbl->schema(), &mb);

  auto inserts_begin =
      inserts_.lower_bound(table_row_pair(tbl, &key_search_row));
  auto inserts_end = inserts_.upper_bound(table_row_pair(tbl, &key_search_row));

  Enumerator<const Row *> *cursor = base_->query(tbl, mb).unbox();
  MergedCursor *merged_cursor =
      new MergedCursor(tbl, cursor, inserts_begin, inserts_end, removes_);
  return ResultSet(merged_cursor);
}


ResultSet TxnNested::query_lt(Table *tbl,
                              const SortedMultiKey &smk,
                              symbol_t order /* =? */) {
  verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC
             || order == symbol_t::ORD_ANY);

  Enumerator<const Row *> *cursor = base_->query_lt(tbl, smk, order).unbox();

  KeyOnlySearchRow key_search_row(tbl->schema(), &smk.get_multi_blob());
  auto inserts_begin =
      inserts_.lower_bound(table_row_pair(tbl, table_row_pair::ROW_MIN));
  auto inserts_end = inserts_.lower_bound(table_row_pair(tbl, &key_search_row));
  MergedCursor *merged_cursor = nullptr;

  if (order == symbol_t::ORD_DESC) {
    auto inserts_rbegin =
        std::multiset<table_row_pair>::const_reverse_iterator(inserts_end);
    auto inserts_rend =
        std::multiset<table_row_pair>::const_reverse_iterator(inserts_begin);
    merged_cursor =
        new MergedCursor(tbl, cursor, inserts_rbegin, inserts_rend, removes_);

  } else {
    merged_cursor =
        new MergedCursor(tbl, cursor, inserts_begin, inserts_end, removes_);
  }

  return ResultSet(merged_cursor);
}

ResultSet TxnNested::query_gt(Table *tbl,
                              const SortedMultiKey &smk,
                              symbol_t order /* =? */) {
  verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC
             || order == symbol_t::ORD_ANY);

  Enumerator<const Row *> *cursor = base_->query_gt(tbl, smk, order).unbox();

  KeyOnlySearchRow key_search_row(tbl->schema(), &smk.get_multi_blob());
  auto inserts_begin =
      inserts_.upper_bound(table_row_pair(tbl, &key_search_row));
  auto inserts_end =
      inserts_.upper_bound(table_row_pair(tbl, table_row_pair::ROW_MAX));
  MergedCursor *merged_cursor = nullptr;

  if (order == symbol_t::ORD_DESC) {
    auto inserts_rbegin =
        std::multiset<table_row_pair>::const_reverse_iterator(inserts_end);
    auto inserts_rend =
        std::multiset<table_row_pair>::const_reverse_iterator(inserts_begin);
    merged_cursor =
        new MergedCursor(tbl, cursor, inserts_rbegin, inserts_rend, removes_);

  } else {
    merged_cursor =
        new MergedCursor(tbl, cursor, inserts_begin, inserts_end, removes_);
  }

  return ResultSet(merged_cursor);
}

ResultSet TxnNested::query_in(Table *tbl,
                              const SortedMultiKey &low,
                              const SortedMultiKey &high,
                              symbol_t order /* =? */) {
  verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC
             || order == symbol_t::ORD_ANY);

  Enumerator<const Row *>
      *cursor = base_->query_in(tbl, low, high, order).unbox();

  MergedCursor *merged_cursor = nullptr;
  KeyOnlySearchRow key_search_row_low(tbl->schema(), &low.get_multi_blob());
  KeyOnlySearchRow key_search_row_high(tbl->schema(), &high.get_multi_blob());
  auto inserts_begin =
      inserts_.upper_bound(table_row_pair(tbl, &key_search_row_low));
  auto inserts_end =
      inserts_.lower_bound(table_row_pair(tbl, &key_search_row_high));

  if (order == symbol_t::ORD_DESC) {
    auto inserts_rbegin =
        std::multiset<table_row_pair>::const_reverse_iterator(inserts_end);
    auto inserts_rend =
        std::multiset<table_row_pair>::const_reverse_iterator(inserts_begin);
    merged_cursor =
        new MergedCursor(tbl, cursor, inserts_rbegin, inserts_rend, removes_);

  } else {
    merged_cursor =
        new MergedCursor(tbl, cursor, inserts_begin, inserts_end, removes_);
  }

  return ResultSet(merged_cursor);
}


ResultSet TxnNested::all(Table *tbl, symbol_t order /* =? */) {
  verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC
             || order == symbol_t::ORD_ANY);

  Enumerator<const Row *> *cursor = base_->all(tbl, order).unbox();

  MergedCursor *merged_cursor = nullptr;
  auto inserts_begin =
      inserts_.lower_bound(table_row_pair(tbl, table_row_pair::ROW_MIN));
  auto inserts_end =
      inserts_.upper_bound(table_row_pair(tbl, table_row_pair::ROW_MAX));

  if (order == symbol_t::ORD_DESC) {
    auto inserts_rbegin =
        std::multiset<table_row_pair>::const_reverse_iterator(inserts_end);
    auto inserts_rend =
        std::multiset<table_row_pair>::const_reverse_iterator(inserts_begin);
    merged_cursor =
        new MergedCursor(tbl, cursor, inserts_rbegin, inserts_rend, removes_);
  } else {
    merged_cursor =
        new MergedCursor(tbl, cursor, inserts_begin, inserts_end, removes_);
  }

  return ResultSet(merged_cursor);
}


} // namespace mdb
