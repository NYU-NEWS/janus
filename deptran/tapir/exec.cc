#include "../__dep__.h"
#include "deptran/procedure.h"
#include "exec.h"
#include "dtxn.h"

namespace rococo {

set<Row*> TapirExecutor::locked_rows_s = {};

void TapirExecutor::FastAccept(const vector<SimpleCommand>& txn_cmds,
                               int32_t* res) {

  *res = SUCCESS;
  if (txn_cmds.size() >= 2) {
    verify(txn_cmds[0].timestamp_ == txn_cmds[1].timestamp_);
  }

  map<int32_t, Value> output_m;
  for (auto& c: txn_cmds) {
    output_m.insert(c.output.begin(), c.output.end());
  }

  // mocking dispatch if not dispatched already.
  // some optimization on early abort.
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
  // my understanding was that this is a wait-die locking for 2PC-prepare.
  // but to be safe, let us follow the stock protocol.
  // validate read versions
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
      verify(col_id < row->prepared_rver_.size());
      verify(col_id < row->prepared_wver_.size());
      if (ver_read < ver_now) {
        // value has been updated. abort transaction.
        *res = REJECT;
        return;
      } else if (ver_read < row->min_prepared_wver(col_id)) {
        // abstain is not very useful for now, as we are not counting the
        // difference between aborts. but let us have it for future.
        *res = ABSTAIN;
        return;
      }
      // record prepared read timestamp
      row->insert_prepared_rver(col_id, ver_read);
      prepared_rvers_[row][col_id] = ver_read;
    }
  }
  // validate write set.
  auto ver_write = txn_cmds[0].timestamp_;
  for (auto& pair1 : dtxn()->write_bufs_) {
    mdb::VersionedRow* row = dynamic_cast<mdb::VersionedRow*>(pair1.first);
    verify(row != nullptr);
    for (auto& pair2: pair1.second) {
      auto col_id = pair2.first;
      auto& value = pair2.second;
      // value's timestamp was not set before, so set here.
      // i do not like this hack, maybe remove the version in value later.
      value.ver_ = ver_write;
      if (ver_write < row->max_prepared_rver(col_id)) {
        *res = RETRY;
      } else if (ver_write < row->ver_[col_id]) {
        *res = RETRY;
      }
      // record prepared write timestamp
      row->insert_prepared_rver(col_id, ver_write);
      prepared_wvers_[row][col_id] = ver_write;
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
      row->set_column_ver(col_id, value.ver_);
    }
  }
  Cleanup();
}

void TapirExecutor::Cleanup() {
  for (auto pair1 : prepared_rvers_) {
    mdb::VersionedRow* row = dynamic_cast<mdb::VersionedRow*>(pair1.first);
    for (auto pair2 : pair1.second) {
      auto col_id = pair2.first;
      auto ver = pair2.second;
      row->remove_prepared_rver(col_id, ver);
    }
  }
  for (auto pair1 : prepared_wvers_) {
    mdb::VersionedRow* row = dynamic_cast<mdb::VersionedRow*>(pair1.first);
    for (auto pair2 : pair1.second) {
      auto col_id = pair2.first;
      auto ver = pair2.second;
      row->remove_prepared_wver(col_id, ver);
    }
  }
  dtxn()->read_vers_.clear();
  dtxn()->write_bufs_.clear();
}

void TapirExecutor::Abort() {
  Cleanup();
}



TapirDTxn* TapirExecutor::dtxn() {
  verify(dtxn_ != nullptr);
  auto d = dynamic_cast<TapirDTxn*>(dtxn_);
  verify(d);
  return d;
}

} // namespace rococo
