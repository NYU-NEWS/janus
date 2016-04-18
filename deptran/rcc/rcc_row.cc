#include "rcc_row.h"
#include "../dtxn.h"

namespace rococo {

void RCCRow::init_dep(int n_columns) {
    dep_entry_ = new entry_t[n_columns];
}

RCCRow::~RCCRow() {
    delete [] dep_entry_;
}

void RCCRow::copy_into(RCCRow * row) const {
    mdb::CoarseLockedRow::copy_into((mdb::CoarseLockedRow *)row);
    int n_columns = schema_->columns_count();
    row->init_dep(n_columns);
    memcpy(row->dep_entry_, dep_entry_, n_columns * sizeof(entry_t));
}

entry_t *RCCRow::get_dep_entry(int col_id) {
    verify(schema_->columns_count() > col_id);
    return dep_entry_ + col_id;
}

} // namespace rcc
