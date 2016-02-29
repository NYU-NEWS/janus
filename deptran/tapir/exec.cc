#include "exec.h"
#include "dtxn.h"

namespace rococo {

void TapirExecutor::FastAccept(int* res) {
  // validate read versions and
  for (auto& pair1 : dtxn()->read_vers_) {
    auto row = (mdb::VersionedRow*)pair1.first;
    for (auto& pair2: pair1.second) {
      auto col_id = pair2.first;
      auto ver = pair2.second;
      if (row->get_column_ver(col_id) > pair2.second) {
        // value has been updated. abort transaction
        *res = REJECT;
        return;
      } else {
        // grab read lock.
        if (!row->rlock_row_by(this->cmd_id_)) {
          *res = REJECT;
        } else {
          // remember locks?
          // TODO
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
        // remember locks. TODO
      }
    }
  }
  *res = SUCCESS;
}

TapirDTxn* TapirExecutor::dtxn() {
  verify(dtxn_ != nullptr);
  return dynamic_cast<TapirDTxn*>(dtxn_);
}

} // namespace rococo