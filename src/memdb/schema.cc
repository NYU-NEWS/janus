#include <set>

#include "schema.h"

using namespace mdb;
using namespace std;

namespace mdb {

int Schema::do_add_column(const char* name, Value::kind type, bool key) {
    colid_t this_column_id = col_name_to_id_.size();
    if (col_name_to_id_.find(name) != col_name_to_id_.end()) {
        return -1;
    }

    column_info col_info;
    col_info.name = name;
    col_info.id = this_column_id;
    col_info.indexed = key;
    col_info.type = type;

    if (col_info.indexed) {
        key_cols_id_.push_back(col_info.id);
    }

    if (col_info.type == Value::STR) {
        // var size
        col_info.var_size_idx = var_size_cols_;
        var_size_cols_++;
    } else {
        // fixed size
        col_info.fixed_size_offst = fixed_part_size_;
        switch (type) {
        case Value::I32:
            fixed_part_size_ += sizeof(i32);
            break;
        case Value::I64:
            fixed_part_size_ += sizeof(i64);
            break;
        case Value::DOUBLE:
            fixed_part_size_ += sizeof(double);
            break;
        default:
            Log::fatal("value type %d not recognized", (int) type);
            verify(0);
            break;
        }
    }

    insert_into_map(col_name_to_id_, string(name), col_info.id);
    col_info_.push_back(col_info);
    return col_info.id;
}

void IndexedSchema::index_sanity_check(const std::vector<colid_t>& idx) {
    set<colid_t> s(idx.begin(), idx.end());
    verify(s.size() == idx.size());
    for (auto& column_id : idx) {
        verify(column_id >= 0 && column_id < (colid_t) columns_count());
    }
}

int IndexedSchema::add_index(const char* name, const std::vector<colid_t>& idx) {
    index_sanity_check(idx);
    int this_idx_id = all_idx_.size();
    if (idx_name_.find(name) != idx_name_.end()) {
        return -1;
    }
    for (auto& col_id : idx) {
        // set up the indexed mark
        col_info_[col_id].indexed = true;
    }
    idx_name_[name] = this_idx_id;
    all_idx_.push_back(idx);
    return this_idx_id;
}

int IndexedSchema::add_index_by_column_names(const char* name, const std::vector<std::string>& named_idx) {
    std::vector<colid_t> idx;
    for (auto& col_name : named_idx) {
        colid_t col_id = this->get_column_id(col_name);
        verify(col_id >= 0);
        idx.push_back(col_id);
    }
    return this->add_index(name, idx);
}

} // namespace mdb
