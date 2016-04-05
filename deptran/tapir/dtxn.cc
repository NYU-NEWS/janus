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
  verify(r->rtti() == symbol_t::ROW_VERSIONED);
  auto c = r->get_column(col_id);
  auto ver_id = r->get_column_ver(col_id);
  row->ref_copy();
  read_vers_[row][col_id] = ver_id;
  *value = c;
  return true;
}

bool TapirDTxn::ReadColumns(Row *row,
                            const std::vector<column_id_t> &col_ids,
                            std::vector<Value> *values,
                            int hint_flag) {
  verify(0);
}

bool TapirDTxn::WriteColumn(Row *row,
                            column_id_t col_id,
                            const Value &value,
                            int hint_flag) {
  row->ref_copy(); // TODO
  write_bufs_[row][col_id] = value;
  return true;
}

bool TapirDTxn::WriteColumns(Row *row,
                             const std::vector<column_id_t> &col_ids,
                             const std::vector<Value> &values,
                             int hint_flag) {
  for (int i = 0; i < col_ids.size(); i++) {
    row->ref_copy(); // TODO
    auto col_id = col_ids[i];
    write_bufs_[row][col_id] = values[i];
  }
  return true;
}

} // namespace rococo
