#include "../__dep__.h"
#include "exec.h"
#include "dtxn.h"

namespace rococo {

void TapirExecutor::FastAccept(const vector<SimpleCommand>& txn_cmds,
                               int32_t* res) {
  // validate read versions and
  *res = SUCCESS;
  for (auto& cmd: txn_cmds) {
    map<int32_t, Value> output;
    int32_t r;
    Execute(cmd, &r, output);
    for (auto& pair : output) {
      verify(cmd.output.size() > 0);
      auto& i = pair.first;
      auto& v = pair.second;
      if (v.ver_ != cmd.output.at(i).ver_) {
        *res = REJECT;
      }
    }
  }
  // TODO
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
        *res = REJECT;
        return;
      } else {
        // grab read lock.
        if (!row->rlock_row_by(this->cmd_id_)) {
          *res = REJECT;
//          verify(0);
        } else {
          // remember locks.
          locked_rows_.insert(row);
        }
      };
    }
  }
  // grab write locks.
  for (auto& pair1 : dtxn()->write_bufs_) {
    auto row = (mdb::VersionedRow*)pair1.first;
    for (auto& pair2: pair1.second) {
      auto col_id = pair2.first;
      if (!row->wlock_row_by(cmd_id_)) {
        *res = REJECT;
      } else {
        // remember locks.
        locked_rows_.insert(row);
      }
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
      row->update(col_id, value);
    }
  }
  // release all the locks.
  for (auto row : locked_rows_) {
    row->unlock_row_by(cmd_id_);
  }
  dtxn()->read_vers_.clear();
  dtxn()->write_bufs_.clear();
}

void TapirExecutor::Abort() {
  for (auto row : locked_rows_) {
    row->unlock_row_by(cmd_id_);
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