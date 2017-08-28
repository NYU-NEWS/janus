#include "../__dep__.h"
#include "tx.h"
#include "frame.h"
#include "scheduler.h"

namespace janus {

using mdb::Value;

TxTapir::~TxTapir() {
}

bool TxTapir::ReadColumn(mdb::Row *row,
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

bool TxTapir::WriteColumn(Row *row,
                            colid_t col_id,
                            const Value &value,
                            int hint_flag) {
  row->ref_copy(); // TODO
  write_bufs_[row][col_id] = value;
  return true;
}

} // namespace janus
