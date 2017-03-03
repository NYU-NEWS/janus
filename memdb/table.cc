#include "utils.h"
#include "table.h"

using namespace std;

namespace mdb {

int SortedMultiKey::compare(const SortedMultiKey& o) const {
    verify(schema_ == o.schema_);
    const std::vector<int>& key_cols = schema_->key_columns_id();
    for (size_t i = 0; i < key_cols.size(); i++) {
        const Schema::column_info* info = schema_->get_column_info(key_cols[i]);
        verify(info->indexed);
        switch (info->type) {
        case Value::I32:
            {
                i32 mine = *(i32 *) mb_[i].data;
                i32 other = *(i32 *) o.mb_[i].data;
                assert(mb_[i].len == (int) sizeof(i32));
                assert(o.mb_[i].len == (int) sizeof(i32));
                if (mine < other) {
                    return -1;
                } else if (mine > other) {
                    return 1;
                }
            }
            break;
        case Value::I64:
            {
                i64 mine = *(i64 *) mb_[i].data;
                i64 other = *(i64 *) o.mb_[i].data;
                assert(mb_[i].len == (int) sizeof(i64));
                assert(o.mb_[i].len == (int) sizeof(i64));
                if (mine < other) {
                    return -1;
                } else if (mine > other) {
                    return 1;
                }
            }
            break;
        case Value::DOUBLE:
            {
                double mine = *(double *) mb_[i].data;
                double other = *(double *) o.mb_[i].data;
                assert(mb_[i].len == (int) sizeof(double));
                assert(o.mb_[i].len == (int) sizeof(double));
                if (mine < other) {
                    return -1;
                } else if (mine > other) {
                    return 1;
                }
            }
            break;
        case Value::STR:
            {
                int min_size = std::min(mb_[i].len, o.mb_[i].len);
                int cmp = memcmp(mb_[i].data, o.mb_[i].data, min_size);
                if (cmp < 0) {
                    return -1;
                } else if (cmp > 0) {
                    return 1;
                }
                // now check who's longer
                if (mb_[i].len < o.mb_[i].len) {
                    return -1;
                } else if (mb_[i].len > o.mb_[i].len) {
                    return 1;
                }
            }
            break;
        default:
            Log::fatal("unexpected column type %d", info->type);
            verify(0);
        }
    }
    return 0;
}


SortedTable::~SortedTable() {
    for (auto& it: rows_) {
        it.second->release();
    }
}

void SortedTable::clear() {
    for (auto& it: rows_) {
        it.second->release();
    }
    rows_.clear();
}

void SortedTable::remove(const SortedMultiKey& smk) {
    auto query_range = rows_.equal_range(smk);
    iterator it = query_range.first;
    while (it != query_range.second) {
        it = remove(it);
    }
}

void SortedTable::remove(Row* row, bool do_free /* =? */) {
    SortedMultiKey smk = SortedMultiKey(row->get_key(), schema_);
    auto query_range = rows_.equal_range(smk);
    iterator it = query_range.first;
    while (it != query_range.second) {
        if (it->second == row) {
            it->second->set_table(nullptr);
            it = remove(it, do_free);
            break;
        } else {
            ++it;
        }
    }
}

void SortedTable::remove(Cursor cur) {
    iterator it = cur.begin();
    while (it != cur.end()) {
        it = this->remove(it);
    }
}

SortedTable::iterator SortedTable::remove(iterator it, bool do_free /* =? */) {
    if (it != rows_.end()) {
        if (do_free) {
            it->second->release();
        }
        return rows_.erase(it);
    } else {
        return rows_.end();
    }
}

UnsortedTable::~UnsortedTable() {
    for (auto& it: rows_) {
        it.second->release();
    }
}

void UnsortedTable::clear() {
    for (auto& it: rows_) {
        it.second->release();
    }
    rows_.clear();
}

void UnsortedTable::remove(const MultiBlob& key) {
    auto query_range = rows_.equal_range(key);
    iterator it = query_range.first;
    while (it != query_range.second) {
        it = remove(it);
    }
}

void UnsortedTable::remove(Row* row, bool do_free /* =? */) {
    auto query_range = rows_.equal_range(row->get_key());
    iterator it = query_range.first;
    while (it != query_range.second) {
        if (it->second == row) {
            it->second->set_table(nullptr);
            it = remove(it, do_free);
            break;
        } else {
            ++it;
        }
    }
}

UnsortedTable::iterator UnsortedTable::remove(iterator it, bool do_free /* =? */) {
    if (it != rows_.end()) {
        if (do_free) {
            it->second->release();
        }
        return rows_.erase(it);
    } else {
        return rows_.end();
    }
}


const Schema* Index::get_schema() const {
    return idx_tbl_->index_schemas_[idx_id_];
}

SortedTable* Index::get_index_table() const {
    return idx_tbl_->indices_[idx_id_];
}

Index::Cursor Index::query(const SortedMultiKey& smk) {
    return Index::Cursor(get_index_table()->query(smk));
}

Index::Cursor Index::query_lt(const SortedMultiKey& smk, symbol_t order /* =? */) {
    return Index::Cursor(get_index_table()->query_lt(smk, order));
}

Index::Cursor Index::query_gt(const SortedMultiKey& smk, symbol_t order /* =? */) {
    return Index::Cursor(get_index_table()->query_gt(smk, order));
}

Index::Cursor Index::query_in(const SortedMultiKey& low, const SortedMultiKey& high, symbol_t order /* =? */) {
    return Index::Cursor(get_index_table()->query_in(low, high, order));
}

Index::Cursor Index::all(symbol_t order /* =? */) const {
    return Index::Cursor(get_index_table()->all());
}

void IndexedTable::destroy_secondary_indices(master_index* master_idx) {
    // we stop at id = master_idx->size() - 1, since master_idx.back() is the original Row*
    for (size_t id = 0; id < master_idx->size() - 1; id++) {
        Row* row = master_idx->at(id);
        Table* tbl = const_cast<Table *>(row->get_table());
        if (tbl == nullptr) {
            row->release();
        } else {
            tbl->remove(row, true);
        }
    }
    delete master_idx;
}


Row* IndexedTable::make_index_row(Row* base, int idx_id, master_index* master_idx) {
    vector<Value> idx_keys;

    // pick columns from base row into the index
    for (colid_t col_id : ((IndexedSchema *) schema_)->get_index(idx_id)) {
        Value picked_value = base->get_column(col_id);
        idx_keys.push_back(picked_value);
    }

    // append pointer to master index on Rows in index table
    idx_keys.push_back(Value((i64) master_idx));

    // create index row and insert them into index table
    Schema* idx_schema = index_schemas_[idx_id];
    Row* idx_row = Row::create(idx_schema, idx_keys);

    // register the index row in master_idx
    verify((*master_idx)[idx_id] == nullptr);
    (*master_idx)[idx_id] = idx_row;

    return idx_row;
}


IndexedTable::IndexedTable(std::string name, const IndexedSchema* schema): SortedTable(name, schema) {
    for (auto idx = schema->index_begin(); idx != schema->index_end(); ++idx) {
        Schema* idx_schema = new Schema;
        for (auto& col_id : *idx) {
            auto col_info = schema->get_column_info(col_id);
            idx_schema->add_key_column(col_info->name.c_str(), col_info->type);
        }
        verify(idx_schema->add_column(".hidden", Value::I64) >= 0);
        SortedTable* idx_tbl = new SortedTable(name, idx_schema);
        index_schemas_.push_back(idx_schema);
        indices_.push_back(idx_tbl);
    }
}

IndexedTable::~IndexedTable() {
    for (auto& it: rows_) {
        // get rid of the index
        Value ptr_value = it.second->get_column(index_column_id());
        master_index* idx = (master_index *) ptr_value.get_i64();
        delete idx;
    }
    for (auto& idx_table : indices_) {
        delete idx_table;
    }
    for (auto& idx_schema : index_schemas_) {
        delete idx_schema;
    }
    // NOTE: ~SortedTable() will be called, releasing Rows in table
}

void IndexedTable::insert(Row* row) {
    Value ptr_value = row->get_column(index_column_id());
    if (ptr_value.get_i64() == 0) {
        master_index* master_idx = new master_index(indices_.size() + 1);

        // the last element in master index points back to the base Row
        master_idx->back() = row;

        for (size_t idx_id = 0; idx_id < master_idx->size() - 1; idx_id++) {
            // pointer slots in master_idx will also be updated
            Row* idx_row = make_index_row(row, idx_id, master_idx);

            SortedTable* idx_tbl = indices_[idx_id];
            idx_tbl->insert(idx_row);
        }
        row->update(index_column_id(), (i64) master_idx);
    }
    this->SortedTable::insert(row);
}

void IndexedTable::remove(Index::Cursor idx_cursor) {
    vector<Row*> rows;
    while (idx_cursor) {
        Row* row = const_cast<Row*>(idx_cursor.next());
        rows.push_back(row);
    }
    for (auto& row : rows) {
        remove(row);
    }
}

IndexedTable::iterator IndexedTable::remove(iterator it, bool do_free /* =? */) {
    if (it != rows_.end()) {
        if (do_free) {
            Row* row = it->second;
            Value ptr_value = row->get_column(index_column_id());
            master_index* idx = (master_index *) ptr_value.get_i64();
            destroy_secondary_indices(idx);
            row->release();
        }
        return rows_.erase(it);
    } else {
        return rows_.end();
    }
}

void IndexedTable::notify_before_update(Row* row, int updated_column_id) {
    verify(row->get_table() == this);

    Value ptr_value = row->get_column(index_column_id());
    master_index* master_idx = (master_index *) ptr_value.get_i64();
    verify(master_idx != nullptr);

    // remove the affected secondary indices
    for (size_t idx_id = 0; idx_id < indices_.size(); idx_id++) {
        bool affected = false;
        for (colid_t col_id : ((IndexedSchema *) schema_)->get_index(idx_id)) {
            if (updated_column_id == col_id) {
                affected = true;
                break;
            }
        }
        if (!affected) {
            continue;
        }
        Row* affected_index_row = master_idx->at(idx_id);
        verify(affected_index_row != nullptr);

        // erase affected index row
        SortedTable* idx_tbl = indices_[idx_id];
        idx_tbl->remove(affected_index_row, true);

        // also remove the pointer
        (*master_idx)[idx_id] = nullptr;
    }
}

void IndexedTable::notify_after_update(Row* row, int updated_column_id) {
    verify(row->get_table() == this);

    Value ptr_value = row->get_column(index_column_id());
    master_index* master_idx = (master_index *) ptr_value.get_i64();
    verify(master_idx != nullptr);

    // re-insert the affected secondary indices
    for (size_t idx_id = 0; idx_id < indices_.size(); idx_id++) {
        bool affected = false;
        for (colid_t col_id : ((IndexedSchema *) schema_)->get_index(idx_id)) {
            if (updated_column_id == col_id) {
                affected = true;
                break;
            }
        }
        if (!affected) {
            continue;
        }

        // NOTE: master index pointers will be updated by make_index_row, so we don't need to explicitly write it
        Row* reconstructed_index_row = make_index_row(row, idx_id, master_idx);
        verify(master_idx->at(idx_id) != nullptr);

        // re-insert reconstructed index row
        SortedTable* idx_tbl = indices_[idx_id];
        idx_tbl->insert(reconstructed_index_row);
    }
}


} // namespace mdb
