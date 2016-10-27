#include "__dep__.h"
#include "../config.h"
#include "../multi_value.h"
#include "../txn_chopper.h"
#include "../rcc/dep_graph.h"
#include "../rcc/graph_marshaler.h"
#include "exec.h"
#include "sched.h"

namespace rococo {

int OCCExecutor::OnDispatch(const SimpleCommand &cmd,
                            rrr::i32 *res,
                            map<int32_t, Value> *output,
                            const function<void()> &callback) {
  this->Execute(cmd, res, *output);
  callback();
  return 0;
}


bool OCCExecutor::Prepare() {
  verify(Config::config_s->cc_mode_ == MODE_OCC);
  auto txn = (mdb::TxnOCC *)mdb_txn();
  verify(txn != NULL);

  verify(txn->outcome_ == symbol_t::NONE);
  verify(!txn->verified_);

  if (!txn->version_check()) {
    Log_debug("txn: %llx occ validation failed.", (int64_t)cmd_id_);
    return false;
  } else {
    // now lock the commit
    for (auto &it : txn->ver_check_read_) {
      Row *row = it.first.row;
      VersionedRow *v_row = (VersionedRow *) row;
      Log_debug("r_lock row: %llx", row);
      if (!v_row->rlock_row_by(txn->id())) {
#ifdef CONFLICT_COUNT
        const Table *tbl = v_row->get_table();
      auto cc_it = TxnMgr::rl_conflict_count_.find(tbl);
      if (cc_it == TxnMgr::rl_conflict_count_.end())
          TxnMgr::rl_conflict_count_[tbl] = 1;
      else
          cc_it->second++;
#endif
        for (auto &lit : txn->locks_) {
          Row *row = lit.first;
          verify(row->rtti() == symbol_t::ROW_VERSIONED);
          VersionedRow *v_row = (VersionedRow *) row;
          v_row->unlock_row_by(txn->id());
        }
        txn->locks_.clear();
        Log_debug("txn: %llx occ read locks failed.", (int64_t)cmd_id_);
        return false;
      }
      insert_into_map(txn->locks_, row, -1);
    }
    for (auto &it : txn->updates_) {
      Row *row = it.first;
      VersionedRow *v_row = (VersionedRow *) row;
      Log_debug("w_lock row: %llx", row);
      if (!v_row->wlock_row_by(txn->id())) {
#ifdef CONFLICT_COUNT
        const Table *tbl = v_row->get_table();
      auto cc_it = TxnMgr::wl_conflict_count_.find(tbl);
      if (cc_it == TxnMgr::wl_conflict_count_.end())
          TxnMgr::wl_conflict_count_[tbl] = 1;
      else
          cc_it->second++;
#endif
        for (auto &lit : txn->locks_) {
          Row *row = lit.first;
          verify(row->rtti() == symbol_t::ROW_VERSIONED);
          VersionedRow *v_row = (VersionedRow *) row;
          v_row->unlock_row_by(txn->id());
        }
        txn->locks_.clear();
        Log_debug("txn: %llx occ write locks failed.", (int64_t)cmd_id_);
        return false;
      }
      insert_into_map(txn->locks_, row, -1);
    }
    Log_debug("txn: %llx occ locks succeed.", (int64_t)cmd_id_);
    txn->verified_ = true;
    return true;
  }
  verify(0);
  return false;
}


int OCCExecutor::Commit() {
  verify(mdb_txn() != nullptr);
  verify(mdb_txn_ == sched_->RemoveMTxn(cmd_id_));
  Log_debug("txn: %llx commit.", (int64_t)cmd_id_);

  auto txn = dynamic_cast<mdb::TxnOCC*>(mdb_txn_);
  verify(txn->outcome_ == symbol_t::NONE);
  verify(txn->verified_);

  for (auto &it : txn->inserts_) {
    it.table->insert(it.row);
  }
  for (auto it = txn->updates_.begin(); it != txn->updates_.end(); /* no ++it! */) {
    Row *row = it->first;
    verify(row->rtti() == mdb::ROW_VERSIONED);
    VersionedRow *v_row = (VersionedRow *) row;
    const Table *tbl = row->get_table();
    if (tbl->rtti() == mdb::TBL_SNAPSHOT) {
      // update on snapshot table (remove then insert)
      Row *new_row = row->copy();
      VersionedRow *v_new_row = (VersionedRow *) new_row;

      // batch update all values
      auto it_end = txn->updates_.upper_bound(row);
      while (it != it_end) {
        column_id_t column_id = it->second.first;
        Value &value = it->second.second;
        new_row->update(column_id, value);
        if (txn->policy_ == symbol_t::OCC_LAZY) {
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

      redirect_locks(txn->locks_, new_row, row);

      // redirect the accessed_rows_
      auto it_accessed = txn->accessed_rows_.find(row);
      if (it_accessed != txn->accessed_rows_.end()) {
        (*it_accessed)->release();
        txn->accessed_rows_.erase(it_accessed);
        new_row->ref_copy();
        txn->accessed_rows_.insert(new_row);
      }
    } else {
      column_id_t column_id = it->second.first;
      Value &value = it->second.second;
      row->update(column_id, value);
      if (txn->policy_ == symbol_t::OCC_LAZY) {
        v_row->incr_column_ver(column_id);
      }
      ++it;
    }
  }
  for (auto &it : txn->removes_) {
    if (txn->policy_ == symbol_t::OCC_LAZY) {
      Row *row = it.row;
      verify(row->rtti() == symbol_t::ROW_VERSIONED);
      VersionedRow *v_row = (VersionedRow *) row;
      for (size_t col_id = 0; col_id < v_row->schema()->columns_count();
           col_id++) {
        v_row->incr_column_ver(col_id);
      }
    }
    // remove the locks since the row has gone already
    txn->locks_.erase(it.row);
    it.table->remove(it.row);
  }
  txn->outcome_ = symbol_t::TXN_COMMIT;
  txn->release_resource();


  delete mdb_txn_;
  mdb_txn_ = nullptr;
  return SUCCESS;
}


} // namespace rococo