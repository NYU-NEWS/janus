#include "value.h"
#include "row.h"
#include "schema.h"
#include "table.h"

namespace mdb {

Row::~Row() {
    delete[] fixed_part_;
    if (schema_->var_size_cols_ > 0) {
        if (kind_ == DENSE) {
            delete[] dense_var_part_;
            delete[] dense_var_idx_;
        } else {
            verify(kind_ == SPARSE);
            delete[] sparse_var_;
        }
    }
}

void Row::copy_into(Row* row) const {
    row->fixed_part_ = new char[this->schema_->fixed_part_size_];
    memcpy(row->fixed_part_, this->fixed_part_, this->schema_->fixed_part_size_);

    row->kind_ = DENSE; // always make a dense copy

    int var_part_size = 0;
    int var_count = 0;
    for (auto& it : *this->schema_) {
        if (it.type != Value::STR) {
            continue;
        }
        var_part_size += this->get_blob(it.id).len;
        var_count++;
    }
    row->dense_var_part_ = new char[var_part_size];
    row->dense_var_idx_ = new int[var_count];

    int var_idx = 0;
    int var_pos = 0;
    for (auto& it : *this->schema_) {
        if (it.type != Value::STR) {
            continue;
        }
        blob b = this->get_blob(it.id);
        memcpy(&row->dense_var_part_[var_pos], b.data, b.len);
        var_pos += b.len;
        row->dense_var_idx_[var_idx] = var_pos;
        var_idx++;
    }

    row->tbl_ = nullptr;    // do not mark it as inside some table

    row->rdonly_ = false;   // always make it writable
    row->schema_ = this->schema_;
}

void Row::make_sparse() {
    if (kind_ == SPARSE) {
        // already sparse data
        return;
    }

    kind_ = SPARSE;

    if (schema_->var_size_cols_ == 0) {
        // special case, no memory copying required
        return;
    }

    // save those 2 values to prevent overwriting (union type!)
    char* var_data = dense_var_part_;
    int* var_idx = dense_var_idx_;

    assert(schema_->var_size_cols_ > 0);
    sparse_var_ = new std::string[schema_->var_size_cols_];
    sparse_var_[0] = std::string(var_data, var_idx[0]);
    for (int i = 1; i < schema_->var_size_cols_; i++) {
        int var_start = var_idx[i - 1];
        int var_len = var_idx[i] - var_idx[i - 1];
        sparse_var_[i] = std::string(&var_data[var_start], var_len);
    }

    delete[] var_data;
    delete[] var_idx;
}

Value Row::get_column(int column_id) const {
    Value v;
    verify(schema_);
    const Schema::column_info* info = schema_->get_column_info(column_id);
    blob b = this->get_blob(column_id);
    verify(info != nullptr);
    switch (info->type) {
    case Value::I32:
        v = Value(*((i32*) b.data));
        break;
    case Value::I64:
        v = Value(*((i64*) b.data));
        break;
    case Value::DOUBLE:
        v = Value(*((double*) b.data));
        break;
    case Value::STR:
        v = Value(std::string(b.data, b.len));
        break;
    default:
        Log::fatal("unexpected value type %d", info->type);
        verify(0);
        break;
    }
    return v;
}

MultiBlob Row::get_key() const {
    const std::vector<int>& key_cols = schema_->key_columns_id();
    MultiBlob mb(key_cols.size());
    for (int i = 0; i < mb.count(); i++) {
        mb[i] = this->get_blob(key_cols[i]);
    }
    return mb;
}

blob Row::get_blob(int column_id) const {
    blob b;
    const Schema::column_info* info = schema_->get_column_info(column_id);
    verify(info != nullptr);
    switch (info->type) {
    case Value::I32:
        b.data = &fixed_part_[info->fixed_size_offst];
        b.len = sizeof(i32);
        break;
    case Value::I64:
        b.data = &fixed_part_[info->fixed_size_offst];
        b.len = sizeof(i64);
        break;
    case Value::DOUBLE:
        b.data = &fixed_part_[info->fixed_size_offst];
        b.len = sizeof(double);
        break;
    case Value::STR:
        if (kind_ == DENSE) {
            int var_start = 0;
            int var_len = 0;
            if (info->var_size_idx == 0) {
                var_start = 0;
                var_len = dense_var_idx_[0];
            } else {
                var_start = dense_var_idx_[info->var_size_idx - 1];
                var_len = dense_var_idx_[info->var_size_idx] - dense_var_idx_[info->var_size_idx - 1];
            }
            b.data = &dense_var_part_[var_start];
            b.len = var_len;
        } else {
            verify(kind_ == SPARSE);
            b.data = &(sparse_var_[info->var_size_idx][0]);
            b.len = sparse_var_[info->var_size_idx].size();
        }
        break;
    default:
        Log::fatal("unexpected value type %d", info->type);
        verify(0);
        break;
    }
    return b;
}

void Row::update_fixed(const Schema::column_info* col, void* ptr, int len) {
    verify(!rdonly_);
    // check if really updating (new data!), and if necessary to remove/insert into table
    bool re_insert = false;
    if (memcmp(&fixed_part_[col->fixed_size_offst], ptr, len) == 0) {
        // not really updating
        return;
    }

    if (col->indexed) {
        re_insert = true;
    }

    // save tbl_, because tbl_->remove() will set it to nullptr
    Table* tbl = tbl_;
    if (re_insert && tbl != nullptr) {
        tbl->notify_before_update(this, col->id);
        tbl->remove(this, false);
    }

    memcpy(&fixed_part_[col->fixed_size_offst], ptr, len);

    if (re_insert && tbl != nullptr) {
        tbl->insert(this);
        tbl->notify_after_update(this, col->id);
    }
}

void Row::update(int column_id, const std::string& v) {
    verify(!rdonly_);
    const Schema::column_info* col = schema_->get_column_info(column_id);
    verify(col->type == Value::STR);

    // check if really updating (new data!), and if necessary to remove/insert into table
    bool re_insert = false;

    blob b;
    if (kind_ == SPARSE) {
        if (this->sparse_var_[col->var_size_idx] == v) {
            return;
        }
    } else {
        verify(kind_ == DENSE);
        b = this->get_blob(column_id);
        if (size_t(b.len) == v.size() && memcmp(b.data, &v[0], v.size()) == 0) {
            return;
        }
    }

    if (col->indexed) {
        re_insert = true;
    }

    // save tbl_, because tbl_->remove() will set it to nullptr
    Table* tbl = tbl_;
    if (re_insert && tbl != nullptr) {
        tbl->notify_before_update(this, column_id);
        tbl->remove(this, false);
    }

    if (kind_ == DENSE && size_t(b.len) == v.size()) {
        // tiny optimization: in-place update if string size is not changed
        memcpy(const_cast<char *>(b.data), &v[0], b.len);
    } else {
        this->make_sparse();
        this->sparse_var_[col->var_size_idx] = v;
    }

    if (re_insert && tbl != nullptr) {
        tbl->insert(this);
        tbl->notify_after_update(this, column_id);
    }
}

void Row::update(int column_id, const Value& v) {
    switch (v.get_kind()) {
    case Value::I32:
        this->update(column_id, v.get_i32());
        break;
    case Value::I64:
        this->update(column_id, v.get_i64());
        break;
    case Value::DOUBLE:
        this->update(column_id, v.get_double());
        break;
    case Value::STR:
        this->update(column_id, v.get_str());
        break;
    default:
        Log::fatal("unexpected value type %d", v.get_kind());
        verify(0);
        break;
    }
}

int Row::compare(const Row& o) const {
    if (&o == this) {
        return 0;
    }
    verify(schema_ == o.schema_);

    // compare based on keys
    SortedMultiKey mine(this->get_key(), schema_);
    SortedMultiKey other(o.get_key(), o.schema_);
    return mine.compare(other);
}


Row* Row::create(Row* raw_row, const Schema* schema, const std::vector<const Value*>& values) {
    Row* row = raw_row;
    row->schema_ = schema;
    row->fixed_part_ = new char[schema->fixed_part_size_];
    memset(row->fixed_part_, 0, schema->fixed_part_size_);
    if (schema->var_size_cols_ > 0) {
        row->dense_var_idx_ = new int[schema->var_size_cols_];
    }

    // 1st pass, write fixed part, and calculate var part size
    int var_part_size = 0;
    int fixed_pos = 0;
    for (auto& it: values) {
        switch (it->get_kind()) {
        case Value::I32:
            it->write_binary(&row->fixed_part_[fixed_pos]);
            fixed_pos += sizeof(i32);
            break;
        case Value::I64:
            it->write_binary(&row->fixed_part_[fixed_pos]);
            fixed_pos += sizeof(i64);
            break;
        case Value::DOUBLE:
            it->write_binary(&row->fixed_part_[fixed_pos]);
            fixed_pos += sizeof(double);
            break;
        case Value::STR:
            var_part_size += it->get_str().size();
            break;
        default:
            Log::fatal("unexpected value type %d", it->get_kind());
            verify(0);
            break;
        }
    }
    for (size_t i = values.size(); i < schema->col_info_.size(); i++) {
        // fake advancing fixed_pos on hidden columns
        switch (schema->col_info_[i].type) {
        case Value::I32:
            fixed_pos += sizeof(i32);
            break;
        case Value::I64:
            fixed_pos += sizeof(i64);
            break;
        case Value::DOUBLE:
            fixed_pos += sizeof(double);
            break;
        case Value::STR:
            break;
        default:
            Log::fatal("unexpected value type %d", schema->col_info_[i].type);
            verify(0);
            break;
        }
    }
    verify(fixed_pos == schema->fixed_part_size_);

    if (schema->var_size_cols_ > 0) {
        // 2nd pass, write var part
        int var_counter = 0;
        int var_pos = 0;
        row->dense_var_part_ = new char[var_part_size];
        for (auto& it: values) {
            if (it->get_kind() == Value::STR) {
                it->write_binary(&row->dense_var_part_[var_pos]);
                var_pos += it->get_str().size();
                row->dense_var_idx_[var_counter] = var_pos;
                var_counter++;
            }
        }
        verify(var_part_size == var_pos);
        for (size_t i = values.size(); i < schema->col_info_.size(); i++) {
            if (schema->col_info_[i].type == Value::STR) {
                // create an empty var column
                row->dense_var_idx_[var_counter] = row->dense_var_idx_[var_counter - 1];
                var_counter++;
            }
        }
        verify(var_counter == schema->var_size_cols_);
    }
    return row;
}

// **** deprecated **** //
FineLockedRow::type_2pl_t FineLockedRow::type_2pl_ = FineLockedRow::TIMEOUT;
uint64_t FineLockedRow::reg_rlock(colid_t column_id,
        std::function<void(uint64_t)> succ_callback,
        std::function<void(void)> fail_callback) {
    verify(0);
    //return lock_[column_id]->lock(succ_callback, fail_callback, rrr::ALock::RLOCK);
}

uint64_t FineLockedRow::reg_wlock(colid_t column_id,
        std::function<void(uint64_t)> succ_callback,
        std::function<void(void)> fail_callback) {
    verify(0);
    //return lock_[column_id]->lock(succ_callback, fail_callback, rrr::ALock::WLOCK);
}

void FineLockedRow::abort_lock_req(colid_t column_id, uint64_t lock_req_id) {
    verify(0);
    //lock_[column_id]->abort(lock_req_id);
}

void FineLockedRow::unlock_column_by(colid_t column_id, uint64_t lock_req_id) {
    verify(0);
    //lock_[column_id]->abort(lock_req_id);
}

// **** deprecated **** //

} // namespace mdb

