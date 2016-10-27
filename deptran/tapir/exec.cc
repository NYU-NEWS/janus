#include "../__dep__.h"
#include "../txn_chopper.h"
#include "exec.h"
#include "dtxn.h"

namespace rococo {

set<Row*> TapirExecutor::locked_rows_s = {};

void TapirExecutor::FastAccept(const vector<SimpleCommand>& txn_cmds,
                               int32_t* res) {
  // validate read versions
  *res = SUCCESS;

  map<int32_t, Value> output_m;
  for (auto& c: txn_cmds) {
    output_m.insert(c.output.begin(), c.output.end());
  }

  TxnOutput output;
  Execute(txn_cmds, &output);
  for (auto& pair : output) {
    for (auto& ppair: pair.second) {
      auto& idx = ppair.first;
      Value& value = ppair.second;
      if (output_m.count(idx) == 0 ||
          value.ver_ != output_m.at(idx).ver_) {
        *res = REJECT;
        return;
      }
    }
  }

  // TODO lock read items.
  auto& read_vers = dtxn()->read_vers_;
  for (auto& pair1 : read_vers) {
    Row* r = pair1.first;
    if (r->rtti() != symbol_t::ROW_VERSIONED) {
      verify(0);
    };
    auto row = dynamic_cast<mdb::VersionedRow*>(r);
    verify(row != nullptr);
    for (auto& pair2: pair1.second) {
      auto col_id = pair2.first;
      auto ver_read = pair2.second;
      auto ver_now = row->get_column_ver(col_id);
      if (ver_now > ver_read) {
        // value has been updated. abort transaction
        verify(0);
//        *res = REJECT;
        return;
      } else {
        // grab read lock.
        if (!row->rlock_row_by(this->cmd_id_)) {
          *res = REJECT;
        } else {
          // remember locks.
          locked_rows_.insert(row);
        }
      };
    }
  }
  // in debug
//  return;
  // grab write locks.
  for (auto& pair1 : dtxn()->write_bufs_) {
    auto row = (mdb::VersionedRow*)pair1.first;
    if (!row->wlock_row_by(cmd_id_)) {
//        verify(0);
      *res = REJECT;
//      Log_info("lock row %llx failed", row);
    } else {
      // remember locks.
//     Log_info("lock row %llx succeed", row);
      locked_rows_.insert(row);
    }
  }
  verify(*res == SUCCESS || *res == REJECT);
}

void TapirExecutor::Commit() {
  // merge write buffers into database.
  for (auto& pair1 : dtxn()->write_bufs_) {
    auto row = (mdb::VersionedRow*)pair1.first;
    for (auto& pair2: pair1.second) {
      auto& col_id = pair2.first;
      auto& value = pair2.second;
      row->incr_column_ver(col_id);
      value.ver_ = row->get_column_ver(col_id);
      row->update(col_id, value);
    }
  }
  // release all the locks.
  for (auto row : locked_rows_) {
    // Log_info("unlock row %llx succeed", row);
    auto r = row->unlock_row_by(cmd_id_);
    verify(r);
  }
  dtxn()->read_vers_.clear();
  dtxn()->write_bufs_.clear();
}

void TapirExecutor::Abort() {
  for (auto row : locked_rows_) {
    // Log_info("unlock row %llx succeed", row);
    auto r = row->unlock_row_by(cmd_id_);
    verify(r);
  }
  dtxn()->read_vers_.clear();
  dtxn()->write_bufs_.clear();
}



TapirDTxn* TapirExecutor::dtxn() {
  verify(dtxn_ != nullptr);
  auto d = dynamic_cast<TapirDTxn*>(dtxn_);
  verify(d);
  return d;
}

} // namespace rococo
