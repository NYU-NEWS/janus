#include "../__dep__.h"
#include "tx_box.h"
#include "frame.h"
#include "scheduler.h"

namespace rococo {

using mdb::Value;

TxBoxTapir::~TxBoxTapir() {
}

bool TxBoxTapir::ReadColumn(mdb::Row *row,
                           mdb::colid_t col_id,
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

bool TxBoxTapir::ReadColumns(Row *row,
                            const std::vector<colid_t> &col_ids,
                            std::vector<Value> *values,
                            int hint_flag) {
  verify(0);
}

bool TxBoxTapir::WriteColumn(Row *row,
                            colid_t col_id,
                            const Value &value,
                            int hint_flag) {
  row->ref_copy(); // TODO
  write_bufs_[row][col_id] = value;
  return true;
}

bool TxBoxTapir::WriteColumns(Row *row,
                             const std::vector<colid_t> &col_ids,
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
