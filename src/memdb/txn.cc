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

Table *Txn::get_table(const std::string &tbl_name) const {
  return mgr_->get_table(tbl_name);
}

SortedTable *Txn::get_sorted_table(const std::string &tbl_name) const {
  return mgr_->get_sorted_table(tbl_name);
}

UnsortedTable *Txn::get_unsorted_table(const std::string &tbl_name) const {
  return mgr_->get_unsorted_table(tbl_name);
}

SnapshotTable *Txn::get_snapshot_table(const std::string &tbl_name) const {
  return mgr_->get_snapshot_table(tbl_name);
}

ResultSet Txn::query(Table *tbl, const Value &kv, bool retrieve, int64_t pid) {
  switch (rtti()) {
    case symbol_t::TXN_2PL:
      return this->query(tbl, kv.get_blob(), retrieve, pid);
    default:
      return this->query(tbl, kv.get_blob());
  }
}



ResultSet Txn::all(Table *tbl, bool retrieve, int64_t pid, symbol_t order) {
  switch (rtti()) {
    case symbol_t::TXN_2PL:
      return all(tbl, retrieve, pid, order);
    default:
      return this->all(tbl, order);
  }
}

ResultSet Txn::query_lt(Table *tbl,
                        const MultiBlob &mb,
                        symbol_t order /* =? */) {
  return query_lt(tbl, SortedMultiKey(mb, tbl->schema()), order);
}

ResultSet Txn::query_lt(Table *tbl,
                        const MultiBlob &mb,
                        bool retrieve,
                        int64_t pid,
                        symbol_t order /* =? */) {
  switch (rtti()) {
    case symbol_t::TXN_2PL:
      return ((Txn2PL *) this)->query_lt(tbl,
                                         SortedMultiKey(mb, tbl->schema()),
                                         retrieve,
                                         pid,
                                         order);
    default:
      return query_lt(tbl, SortedMultiKey(mb, tbl->schema()), order);
  }
}

ResultSet Txn::query_gt(Table *tbl,
                        const MultiBlob &mb,
                        symbol_t order /* =? */) {
  return query_gt(tbl, SortedMultiKey(mb, tbl->schema()), order);
}

ResultSet Txn::query_gt(Table *tbl,
                        const MultiBlob &mb,
                        bool retrieve,
                        int64_t pid,
                        symbol_t order /* =? */) {
  switch (rtti()) {
    case symbol_t::TXN_2PL:
      return ((Txn2PL *) this)->query_gt(tbl,
                                         SortedMultiKey(mb, tbl->schema()),
                                         retrieve,
                                         pid,
                                         order);
    default:
      return query_gt(tbl, SortedMultiKey(mb, tbl->schema()), order);
  }
}

ResultSet Txn::query_in(Table *tbl,
                        const MultiBlob &low,
                        const MultiBlob &high,
                        symbol_t order /* =? */) {
  return query_in(tbl,
                  SortedMultiKey(low, tbl->schema()),
                  SortedMultiKey(high, tbl->schema()),
                  order);
}

ResultSet Txn::query_in(Table *tbl,
                        const MultiBlob &low,
                        const MultiBlob &high,
                        bool retrieve,
                        int64_t pid,
                        symbol_t order /* =? */) {
  switch (rtti()) {
    case symbol_t::TXN_2PL:
      return ((Txn2PL *) this)->query_in(tbl,
                                         SortedMultiKey(low, tbl->schema()),
                                         SortedMultiKey(high, tbl->schema()),
                                         retrieve,
                                         pid,
                                         order);
    default:
      return query_in(tbl,
                      SortedMultiKey(low, tbl->schema()),
                      SortedMultiKey(high, tbl->schema()),
                      order);
  }
}


#ifdef CONFLICT_COUNT
std::map<const Table *, uint64_t> TxnMgr::vc_conflict_count_;
std::map<const Table *, uint64_t> TxnMgr::rl_conflict_count_;
std::map<const Table *, uint64_t> TxnMgr::wl_conflict_count_;
#endif
Txn *TxnMgr::start_nested(Txn *base) {
  return new TxnNested(this, base);
}

UnsortedTable *TxnMgr::get_unsorted_table(const std::string &tbl_name) const {
  Table *tbl = get_table(tbl_name);
  if (tbl != nullptr) {
    verify(tbl->rtti() == TBL_UNSORTED);
  }
  return (UnsortedTable *) tbl;
}

SortedTable *TxnMgr::get_sorted_table(const std::string &tbl_name) const {
  Table *tbl = get_table(tbl_name);
  if (tbl != nullptr) {
    verify(tbl->rtti() == TBL_SORTED);
  }
  return (SortedTable *) tbl;
}

SnapshotTable *TxnMgr::get_snapshot_table(const std::string &tbl_name) const {
  Table *tbl = get_table(tbl_name);
  if (tbl != nullptr) {
    verify(tbl->rtti() == TBL_SNAPSHOT);
  }
  return (SnapshotTable *) tbl;
}



bool table_row_pair::operator<(const table_row_pair &o) const {
  if (table != o.table) {
    return table < o.table;
  } else {
    // we use ROW_MIN and ROW_MAX as special markers
    // this helps to get a range query on staged insert set
    if (row == ROW_MIN) {
      return o.row != ROW_MIN;
    } else if (row == ROW_MAX) {
      return false;
    } else if (o.row == ROW_MIN) {
      return false;
    } else if (o.row == ROW_MAX) {
      return row != ROW_MAX;
    }
    return (*row) < (*o.row);
  }
}

Row *table_row_pair::ROW_MIN = (Row *) 0;
Row *table_row_pair::ROW_MAX = (Row *) ~0;



} // namespace mdb
