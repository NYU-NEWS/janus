#include <limits>

#include "row.h"
#include "table.h"
#include "txn.h"
#include "txn_2pl.h"
#include "txn_occ.h"

namespace mdb {

TxnOCC::TxnOCC(const TxnMgr *mgr,
               txn_id_t txnid,
               const std::vector<std::string> &table_names) : Txn2PL(mgr,
                                                                     txnid),
                                                              verified_(false) {
  for (auto &it: table_names) {
    SnapshotTable *tbl = get_snapshot_table(it);
    SnapshotTable *snapshot = tbl->snapshot();
    insert_into_map(snapshots_, it, snapshot);
    snapshot_tables_.insert(snapshot);
  }
}

TxnOCC::~TxnOCC() {
  release_resource();
}

void TxnOCC::incr_row_refcount(Row *r) {
  if (accessed_rows_.find(r) == accessed_rows_.end()) {
    r->ref_copy();
    accessed_rows_.insert(r);
  }
}

bool TxnOCC::version_check() {
  if (is_readonly()) {
    return true;
  }
  __DebugVersionCheck();
  return version_check(ver_check_read_) && version_check(ver_check_write_);
}

bool TxnOCC::__DebugVersionCheck() {
  for (auto& pair: ver_check_read_) {
    auto& row_col = pair.first;
    auto& ver_read = pair.second;
    auto it = ver_check_write_.find(row_col);
    if (it != ver_check_write_.end()) {
      auto& ver_write = it->second;
      if (ver_read < ver_write) {
        Row* r = row_col.row;
        int c = row_col.col_id;
        uint64_t iii = id();
        Log_debug("Txnid %lx, Table Name %s, col_id %d",
                  iii, r->get_table()->Name().c_str(), c);
        verify(ver_read >= ver_write);
      }
    }
  }
  return true;
}

bool TxnOCC::version_check(const std::unordered_map<row_column_pair,
                                                    version_t,
                                                    row_column_pair::hash> &ver_info) {
  for (auto &it : ver_info) {
    Row *row = it.first.row;
    colid_t col_id = it.first.col_id;
    version_t ver = it.second;
    verify(row->rtti() == ROW_VERSIONED);
    VersionedRow *v_row = (VersionedRow *) row;
    auto curr_ver = v_row->get_column_ver(col_id);
    if (curr_ver != ver) {
#ifdef CONFLICT_COUNT
      const Table *tbl = v_row->get_table();
      auto cc_it = TxnMgr::vc_conflict_count_.find(tbl);
      if (cc_it == TxnMgr::vc_conflict_count_.end())
          TxnMgr::vc_conflict_count_[tbl] = 1;
      else
          cc_it->second++;
#endif
      return false;
    }
  }
  return true;
}

void TxnOCC::release_resource() {
  updates_.clear();
  inserts_.clear();
  removes_.clear();

  for (auto &it : locks_) {
    Row *row = it.first;
    verify(row->rtti() == symbol_t::ROW_VERSIONED);
    VersionedRow *v_row = (VersionedRow *) row;
    v_row->unlock_row_by(this->id());
  }
  locks_.clear();

  ver_check_read_.clear();
  ver_check_write_.clear();

  // release ref copy
  for (auto &it: accessed_rows_) {
    it->release();
  }
  accessed_rows_.clear();

  // release snapshots
  for (auto &it: snapshot_tables_) {
    delete it;
  }
  snapshot_tables_.clear();
  snapshots_.clear();
}

void TxnOCC::abort() {
  verify(outcome_ == symbol_t::NONE);
  outcome_ = symbol_t::TXN_ABORT;
  release_resource();
}


bool TxnOCC::commit() {
  verify(outcome_ == symbol_t::NONE);

  if (!this->version_check()) {
    return false;
  }
  verified_ = true;

  this->commit_confirm();
  return true;
}


bool TxnOCC::commit_prepare() {
verify(0);
}

void TxnOCC::commit_confirm() {
verify(0);
}


bool TxnOCC::read_column(Row *row, colid_t col_id, Value *value) {
  if (is_readonly()) {
    *value = row->get_column(col_id);
    return true;
  }

  assert(debug_check_row_valid(row));
  verify(outcome_ == symbol_t::NONE);

  if (row->get_table() == nullptr) {
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

  // reading from actual table data, track version
  if (row->rtti() == symbol_t::ROW_VERSIONED) {
    VersionedRow *v_row = (VersionedRow *) row;
    insert_into_map(ver_check_read_,
                    row_column_pair(v_row, col_id),
                    v_row->get_column_ver(col_id));
    // increase row reference count because later we are going to check its version
    incr_row_refcount(row);
    __DebugCheckReadVersion(row_column_pair(v_row, col_id),
                            v_row->get_column_ver(col_id));
  } else {
    verify(row->rtti() == symbol_t::ROW_VERSIONED);
  }
  *value = row->get_column(col_id);
  insert_into_map(reads_, row, col_id);

  return true;
}

bool TxnOCC::__DebugCheckReadVersion(row_column_pair row_col,
                                     version_t ver_now) {
  // v is current version in database (or the new row)
  auto it = ver_check_read_.find(row_col);
  if (it != ver_check_read_.end()) {
    auto ver_read = it->second;
    verify(ver_read >= ver_now);
  }
  return true;
}

bool TxnOCC::write_column(Row *row, colid_t col_id, const Value &value) {
  verify(!is_readonly());
  assert(debug_check_row_valid(row));
  verify(outcome_ == symbol_t::NONE);

  if (row->get_table() == nullptr) {
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

  // update staging area, track version
  if (row->rtti() == symbol_t::ROW_VERSIONED) {
    VersionedRow *v_row = (VersionedRow *) row;
    if (policy_ == symbol_t::OCC_EAGER) {
      verify(0);
      v_row->incr_column_ver(col_id);
    }
    version_t ver_now = v_row->get_column_ver(col_id);
    auto row_col = row_column_pair(v_row, col_id);
    __DebugCheckReadVersion(row_col, ver_now);
//    insert_into_map(ver_check_write_, row_col, ver_now); // ???
    // increase row reference count because later we are going to check its version
    incr_row_refcount(row);
  } else {
    // row must either be FineLockedRow or CoarseLockedRow
    verify(row->rtti() == symbol_t::ROW_VERSIONED);
  }
  insert_into_map(updates_, row, std::make_pair(col_id, value));

  return true;
}

bool TxnOCC::insert_row(Table *tbl, Row *row) {
  verify(!is_readonly());
  verify(outcome_ == symbol_t::NONE);
  verify(row->rtti() == symbol_t::ROW_VERSIONED);
  verify(row->get_table() == nullptr);

  // we dont need to incr_row_refcount(row), because it is
  // only problematic for read/write

  inserts_.insert(table_row_pair(tbl, row));
  removes_.erase(table_row_pair(tbl, row));
  return true;
}

bool TxnOCC::remove_row(Table *tbl, Row *row) {
  verify(!is_readonly());
  assert(debug_check_row_valid(row));
  verify(outcome_ == symbol_t::NONE);

  // we dont need to incr_row_refcount(row), because it is
  // only problematic for read/write

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
    if (row->rtti() == symbol_t::ROW_VERSIONED) {
      VersionedRow *v_row = (VersionedRow *) row;

      // remember whole row version
      for (size_t col_id = 0; col_id < v_row->schema()->columns_count();
           col_id++) {
        if (policy_ == symbol_t::OCC_EAGER) {
          v_row->incr_column_ver(col_id);
        }
        insert_into_map(ver_check_write_,
                        row_column_pair(v_row, col_id),
                        v_row->get_column_ver(col_id));
        // increase row reference count because later we are going to check its version
        incr_row_refcount(row);
      }

    } else {
      // row must either be FineLockedRow or CoarseLockedRow
      verify(row->rtti() == symbol_t::ROW_VERSIONED);
    }
    removes_.insert(table_row_pair(tbl, row));
  } else {
    it->row->release();
    inserts_.erase(it);
  }
  updates_.erase(row);

  return true;
}

} // namespace mdb
