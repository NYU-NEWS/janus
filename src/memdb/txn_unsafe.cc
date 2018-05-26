#include <limits>

#include "row.h"
#include "table.h"
#include "txn.h"
#include "txn_2pl.h"
#include "txn_occ.h"
#include "txn_unsafe.h"

namespace mdb {

bool TxnUnsafe::read_column(Row *row, colid_t col_id, Value *value) {
  *value = row->get_column(col_id);
  // always allowed
  return true;
}

bool TxnUnsafe::write_column(Row *row, colid_t col_id, const Value &value) {
  row->update(col_id, value);
  // always allowed
  return true;
}

bool TxnUnsafe::insert_row(Table *tbl, Row *row) {
  tbl->insert(row);
  // always allowed
  return true;
}

bool TxnUnsafe::remove_row(Table *tbl, Row *row) {
  tbl->remove(row);
  // always allowed
  return true;
}

ResultSet TxnUnsafe::query(Table *tbl, const MultiBlob &mb) {
  // always sendback query result from raw table
  if (tbl->rtti() == TBL_UNSORTED) {
    UnsortedTable *t = (UnsortedTable *) tbl;
    UnsortedTable::Cursor *cursor = new UnsortedTable::Cursor(t->query(mb));
    return ResultSet(cursor);
  } else if (tbl->rtti() == TBL_SORTED) {
    SortedTable *t = (SortedTable *) tbl;
    SortedTable::Cursor *cursor = new SortedTable::Cursor(t->query(mb));
    return ResultSet(cursor);
  } else if (tbl->rtti() == TBL_SNAPSHOT) {
    SnapshotTable *t = (SnapshotTable *) tbl;
    SnapshotTable::Cursor *cursor = new SnapshotTable::Cursor(t->query(mb));
    return ResultSet(cursor);
  } else {
    verify(tbl->rtti() == TBL_UNSORTED || tbl->rtti() == TBL_SORTED
               || tbl->rtti() == TBL_SNAPSHOT);
    return ResultSet(nullptr);
  }
}

ResultSet TxnUnsafe::query_lt(Table *tbl,
                              const SortedMultiKey &smk,
                              symbol_t order /* =? */) {
  // always sendback query result from raw table
  if (tbl->rtti() == TBL_SORTED) {
    SortedTable *t = (SortedTable *) tbl;
    SortedTable::Cursor
        *cursor = new SortedTable::Cursor(t->query_lt(smk, order));
    return ResultSet(cursor);
  } else if (tbl->rtti() == TBL_SNAPSHOT) {
    SnapshotTable *t = (SnapshotTable *) tbl;
    SnapshotTable::Cursor
        *cursor = new SnapshotTable::Cursor(t->query_lt(smk, order));
    return ResultSet(cursor);
  } else {
    // range query only works on sorted and snapshot table
    verify(tbl->rtti() == TBL_SORTED || tbl->rtti() == TBL_SNAPSHOT);
    return ResultSet(nullptr);
  }
}

ResultSet TxnUnsafe::query_gt(Table *tbl,
                              const SortedMultiKey &smk,
                              symbol_t order /* =? */) {
  // always sendback query result from raw table
  if (tbl->rtti() == TBL_SORTED) {
    SortedTable *t = (SortedTable *) tbl;
    SortedTable::Cursor
        *cursor = new SortedTable::Cursor(t->query_gt(smk, order));
    return ResultSet(cursor);
  } else if (tbl->rtti() == TBL_SNAPSHOT) {
    SnapshotTable *t = (SnapshotTable *) tbl;
    SnapshotTable::Cursor
        *cursor = new SnapshotTable::Cursor(t->query_gt(smk, order));
    return ResultSet(cursor);
  } else {
    // range query only works on sorted and snapshot table
    verify(tbl->rtti() == TBL_SORTED || tbl->rtti() == TBL_SNAPSHOT);
    return ResultSet(nullptr);
  }
}

ResultSet TxnUnsafe::query_in(Table *tbl,
                              const SortedMultiKey &low,
                              const SortedMultiKey &high,
                              symbol_t order /* =? */) {
  // always sendback query result from raw table
  if (tbl->rtti() == TBL_SORTED) {
    SortedTable *t = (SortedTable *) tbl;
    SortedTable::Cursor
        *cursor = new SortedTable::Cursor(t->query_in(low, high, order));
    return ResultSet(cursor);
  } else if (tbl->rtti() == TBL_SNAPSHOT) {
    SnapshotTable *t = (SnapshotTable *) tbl;
    SnapshotTable::Cursor
        *cursor = new SnapshotTable::Cursor(t->query_in(low, high, order));
    return ResultSet(cursor);
  } else {
    // range query only works on sorted and snapshot table
    verify(tbl->rtti() == TBL_SORTED || tbl->rtti() == TBL_SNAPSHOT);
    return ResultSet(nullptr);
  }
}


ResultSet TxnUnsafe::all(Table *tbl, symbol_t order /* =? */) {
  // always sendback query result from raw table
  if (tbl->rtti() == TBL_UNSORTED) {
    // unsorted tables only accept ORD_ANY
    verify(order == symbol_t::ORD_ANY);
    UnsortedTable *t = (UnsortedTable *) tbl;
    UnsortedTable::Cursor *cursor = new UnsortedTable::Cursor(t->all());
    return ResultSet(cursor);
  } else if (tbl->rtti() == TBL_SORTED) {
    SortedTable *t = (SortedTable *) tbl;
    SortedTable::Cursor *cursor = new SortedTable::Cursor(t->all(order));
    return ResultSet(cursor);
  } else if (tbl->rtti() == TBL_SNAPSHOT) {
    SnapshotTable *t = (SnapshotTable *) tbl;
    SnapshotTable::Cursor *cursor = new SnapshotTable::Cursor(t->all(order));
    return ResultSet(cursor);
  } else {
    verify(tbl->rtti() == TBL_UNSORTED || tbl->rtti() == TBL_SORTED
               || tbl->rtti() == TBL_SNAPSHOT);
    return ResultSet(nullptr);
  }
}

} // namespace mdb
