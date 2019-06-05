#include "../__dep__.h"
#include "../command.h"
#include "tx.h"
#include "scheduler.h"
#include "coordinator.h"

namespace janus {

bool SchedulerTapir::Guard(Tx &tx, Row *row, int col_id, bool write) {
  // do nothing
  return true;
}

int SchedulerTapir::OnFastAccept(txid_t tx_id,
                                 const vector<TxPieceData> &txn_cmds) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  Log_debug("receive fast accept for cmd_id: %llx", tx_id);
  int ret = SUCCESS;
  // my understanding was that this is a wait-die locking for 2PC-prepare.
  // but to be safe, let us follow the stock protocol.
  // validate read versions
  auto tx = dynamic_pointer_cast<TxTapir>(GetOrCreateTx(tx_id));
  tx->fully_dispatched_->Wait();
  if (tx->aborted_in_dispatch_) {
    return REJECT;
  }
  auto& read_vers = tx->read_vers_;
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
        return REJECT;
      } else if (ver_read < row->min_prepared_wver(col_id)) {
        // abstain is not very useful for now, as we are not counting the
        // difference between aborts. but let us have it for future.
        return ABSTAIN;
      }
      // record prepared read timestamp
      row->insert_prepared_rver(col_id, ver_read);
      tx->prepared_rvers_[row][col_id] = ver_read;
    }
  }
  // validate write set.
  auto ver_write = txn_cmds[0].timestamp_;
  for (auto& pair1 : tx->write_bufs_) {
    mdb::VersionedRow* row = dynamic_cast<mdb::VersionedRow*>(pair1.first);
    verify(row != nullptr);
    for (auto& pair2: pair1.second) {
      auto col_id = pair2.first;
      auto& value = pair2.second;
      // value's timestamp was not set before, so set here.
      // i do not like this hack, maybe remove the version in value later.
      value.ver_ = ver_write;
      if (ver_write < row->max_prepared_rver(col_id)) {
        ret = RETRY;
      } else if (ver_write < row->ver_[col_id]) {
        ret = RETRY;
      }
      // record prepared write timestamp
      row->insert_prepared_rver(col_id, ver_write);
      tx->prepared_wvers_[row][col_id] = ver_write;
    }
  }

  // DEBUG
  verify(txn_cmds.size() > 0);
  for (auto &c: txn_cmds) {

  }
  return ret;
}

int SchedulerTapir::OnDecide(txid_t tx_id,
                             int32_t decision,
                             const function<void()> &callback) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto tx = dynamic_pointer_cast<TxTapir>(GetTx(tx_id));
  if (decision == CoordinatorTapir::Decision::COMMIT) {
#ifdef CHECK_ISO
    MergeDeltas(exec->dtxn_->deltas_);
#endif
    // merge write buffers into database.
    for (auto& pair1 : tx->write_bufs_) {
      auto row = (mdb::VersionedRow*)pair1.first;
      for (auto& pair2: pair1.second) {
        auto& col_id = pair2.first;
        auto& value = pair2.second;
        row->update(col_id, value);
        row->set_column_ver(col_id, value.ver_);
      }
    }
  } else if (decision == CoordinatorTapir::Decision::ABORT) {
  } else {
    verify(0);
  }
  // Clean up
  for (auto pair1 : tx->prepared_rvers_) {
    mdb::VersionedRow* row = dynamic_cast<mdb::VersionedRow*>(pair1.first);
    for (auto pair2 : pair1.second) {
      auto col_id = pair2.first;
      auto ver = pair2.second;
      row->remove_prepared_rver(col_id, ver);
    }
  }
  for (auto pair1 : tx->prepared_wvers_) {
    mdb::VersionedRow* row = dynamic_cast<mdb::VersionedRow*>(pair1.first);
    for (auto pair2 : pair1.second) {
      auto col_id = pair2.first;
      auto ver = pair2.second;
      row->remove_prepared_wver(col_id, ver);
    }
  }
  tx->read_vers_.clear();
  tx->write_bufs_.clear();
  DestroyTx(tx_id);
  callback();
  return 0;
}

} // namespace janus
