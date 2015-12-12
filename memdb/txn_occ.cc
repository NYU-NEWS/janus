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
  return version_check(ver_check_read_) && version_check(ver_check_write_);
}

bool TxnOCC::version_check(const std::unordered_map<row_column_pair,
                                                    version_t,
                                                    row_column_pair::hash> &ver_info) {
  for (auto &it : ver_info) {
    Row *row = it.first.row;
    column_id_t col_id = it.first.col_id;
    version_t ver = it.second;
    verify(row->rtti() == ROW_VERSIONED);
    VersionedRow *v_row = (VersionedRow *) row;
    if (v_row->get_column_ver(col_id) != ver) {
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
  verify(outcome_ == symbol_t::NONE);
  verify(verified_ == false);

  if (!this->version_check()) {
    return false;
  }

  // now lock the commit
  for (auto &it : ver_check_read_) {
    Row *row = it.first.row;
    VersionedRow *v_row = (VersionedRow *) row;
    if (!v_row->rlock_row_by(this->id())) {
#ifdef CONFLICT_COUNT
      const Table *tbl = v_row->get_table();
      auto cc_it = TxnMgr::rl_conflict_count_.find(tbl);
      if (cc_it == TxnMgr::rl_conflict_count_.end())
          TxnMgr::rl_conflict_count_[tbl] = 1;
      else
          cc_it->second++;
#endif
      for (auto &lit : locks_) {
        Row *row = lit.first;
        verify(row->rtti() == symbol_t::ROW_VERSIONED);
        VersionedRow *v_row = (VersionedRow *) row;
        v_row->unlock_row_by(this->id());
      }
      locks_.clear();
      return false;
    }
    insert_into_map(locks_, row, -1);
  }
  for (auto &it : ver_check_write_) {
    Row *row = it.first.row;
    VersionedRow *v_row = (VersionedRow *) row;
    if (!v_row->wlock_row_by(this->id())) {
#ifdef CONFLICT_COUNT
      const Table *tbl = v_row->get_table();
      auto cc_it = TxnMgr::wl_conflict_count_.find(tbl);
      if (cc_it == TxnMgr::wl_conflict_count_.end())
          TxnMgr::wl_conflict_count_[tbl] = 1;
      else
          cc_it->second++;
#endif
      for (auto &lit : locks_) {
        Row *row = lit.first;
        verify(row->rtti() == symbol_t::ROW_VERSIONED);
        VersionedRow *v_row = (VersionedRow *) row;
        v_row->unlock_row_by(this->id());
      }
      locks_.clear();
      return false;
    }
    insert_into_map(locks_, row, -1);
  }

  verified_ = true;
  return true;
}

void TxnOCC::commit_confirm() {
  verify(outcome_ == symbol_t::NONE);
  verify(verified_ == true);

  for (auto &it : inserts_) {
    it.table->insert(it.row);
  }
  for (auto it = updates_.begin(); it != updates_.end(); /* no ++it! */) {
    Row *row = it->first;
    verify(row->rtti() == ROW_VERSIONED);
    VersionedRow *v_row = (VersionedRow *) row;
    const Table *tbl = row->get_table();
    if (tbl->rtti() == TBL_SNAPSHOT) {
      // update on snapshot table (remove then insert)
      Row *new_row = row->copy();
      VersionedRow *v_new_row = (VersionedRow *) new_row;

      // batch update all values
      auto it_end = updates_.upper_bound(row);
      while (it != it_end) {
        column_id_t column_id = it->second.first;
        Value &value = it->second.second;
        new_row->update(column_id, value);
        if (policy_ == symbol_t::OCC_LAZY) {
          // increase version for both old and new row
          // so that other Txn will verify fail on old row
          // and also the version info is passed onto new row
          v_row->incr_column_ver(column_id);
          v_new_row->incr_column_ver(column_id);
        }
        ++it;
      }

      SnapshotTable *ss_tbl = (SnapshotTable *) tbl;
      ss_tbl->remove(row);
      ss_tbl->insert(new_row);

      redirect_locks(locks_, new_row, row);

      // redirect the accessed_rows_
      auto it_accessed = accessed_rows_.find(row);
      if (it_accessed != accessed_rows_.end()) {
        (*it_accessed)->release();
        accessed_rows_.erase(it_accessed);
        new_row->ref_copy();
        accessed_rows_.insert(new_row);
      }
    } else {
      column_id_t column_id = it->second.first;
      Value &value = it->second.second;
      row->update(column_id, value);
      if (policy_ == symbol_t::OCC_LAZY) {
        v_row->incr_column_ver(column_id);
      }
      ++it;
    }
  }
  for (auto &it : removes_) {
    if (policy_ == symbol_t::OCC_LAZY) {
      Row *row = it.row;
      verify(row->rtti() == symbol_t::ROW_VERSIONED);
      VersionedRow *v_row = (VersionedRow *) row;
      for (size_t col_id = 0; col_id < v_row->schema()->columns_count();
           col_id++) {
        v_row->incr_column_ver(col_id);
      }
    }
    // remove the locks since the row has gone already
    locks_.erase(it.row);
    it.table->remove(it.row);
  }
  outcome_ = symbol_t::TXN_COMMIT;
  release_resource();
}


bool TxnOCC::read_column(Row *row, column_id_t col_id, Value *value) {
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

  } else {
    verify(row->rtti() == symbol_t::ROW_VERSIONED);
  }
  *value = row->get_column(col_id);
  insert_into_map(reads_, row, col_id);

  return true;
}

bool TxnOCC::write_column(Row *row, column_id_t col_id, const Value &value) {
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
      v_row->incr_column_ver(col_id);
    }
    insert_into_map(ver_check_write_,
                    row_column_pair(v_row, col_id),
                    v_row->get_column_ver(col_id));
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