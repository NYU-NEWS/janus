#include "../__dep__.h"
#include "dtxn.h"
#include "frame.h"
#include "sched.h"

namespace rococo {

using mdb::Value;

TapirDTxn::~TapirDTxn() {
}

bool TapirDTxn::ReadColumn(mdb::Row *row,
                           mdb::column_id_t col_id,
                           Value *value,
                           int hint_flag) {
  // Ignore hint flags.
  auto r = dynamic_cast<mdb::VersionedRow*>(row);
  auto c = r->get_column(col_id);
  auto ver_id = r->get_column_ver(col_id);
  read_vers_[row][col_id] = ver_id;
  *value = c;
  return true;
}

bool TapirDTxn::ReadColumns(Row *row,
                            const std::vector<column_id_t> &col_ids,
                            std::vector<Value> *values,
                            int hint_flag) {
  auto r = dynamic_cast<mdb::VersionedRow*>(row);
  for (auto col_id: col_ids) {
    auto c = r->get_column(col_id);
    auto ver_id = r->get_column_ver(col_id);
    read_vers_[row][col_id] = ver_id;
  }
  return true;
}

bool TapirDTxn::WriteColumn(Row *row,
                            column_id_t col_id,
                            const Value &value,
                            int hint_flag) {
  write_bufs_[row][col_id] = value;
  return true;
}

bool TapirDTxn::WriteColumns(Row *row,
                             const std::vector<column_id_t> &col_ids,
                             const std::vector<Value> &values,
                             int hint_flag) {
  for (int i = 0; i < col_ids.size(); i++) {
    auto col_id = col_ids[i];
    write_bufs_[row][col_id] = values[i];
  }
  return true;
}

} // namespace rococo
