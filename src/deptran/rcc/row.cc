#include "row.h"
#include "../tx.h"

namespace janus {

void RccRow::init_dep(int n_columns) {
    dep_entry_ = new entry_t[n_columns];
}

RccRow::~RccRow() {
    delete [] dep_entry_;
}

void RccRow::copy_into(RccRow * row) const {
    mdb::CoarseLockedRow::copy_into((mdb::CoarseLockedRow *)row);
    int n_columns = schema_->columns_count();
    row->init_dep(n_columns);
    memcpy(row->dep_entry_, dep_entry_, n_columns * sizeof(entry_t));
}

entry_t *RccRow::get_dep_entry(int col_id) {
    verify(schema_->columns_count() > col_id);
    return dep_entry_ + col_id;
}

} // namespace janus
