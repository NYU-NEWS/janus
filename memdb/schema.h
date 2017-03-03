#pragma once

#include <unordered_map>
#include <string>
#include <vector>

#include "value.h"
#include "utils.h"

namespace mdb {

class Row;

class Schema {
    friend class Row;

public:

    struct column_info {
        column_info(): id(-1), indexed(false), type(Value::UNKNOWN), fixed_size_offst(-1) {}

        colid_t id;
        std::string name;
        bool indexed;   // primary index or secondary index
        Value::kind type;

        union {
            // if fixed size (i32, i64, double)
            int fixed_size_offst;

            // if not fixed size (str)
            // need to lookup a index table on row
            int var_size_idx;
        };
    };

    Schema(): var_size_cols_(0), fixed_part_size_(0), hidden_fixed_(0), hidden_var_(0), frozen_(false) {}
    virtual ~Schema() {}

    int add_column(const char* name, Value::kind type, bool key = false) {
        verify(!frozen_ && hidden_fixed_ == 0 && hidden_var_ == 0);
        return do_add_column(name, type, key);  // key: primary index only
    }
    int add_key_column(const char* name, Value::kind type) {
        // key: primary index only
        return add_column(name, type, true);
    }

    colid_t get_column_id(const std::string& name) const {
        auto it = col_name_to_id_.find(name);
        if (it != std::end(col_name_to_id_)) {
            assert(col_info_[it->second].id == it->second);
            return it->second;
        }
        return -1;
    }
    const std::vector<colid_t>& key_columns_id() const {
        // key: primary index only
        return key_cols_id_;
    }

    const column_info* get_column_info(const std::string& name) const {
        colid_t col_id = get_column_id(name);
        if (col_id < 0) {
            return nullptr;
        } else {
            return &col_info_[col_id];
        }
    }
    const column_info* get_column_info(colid_t column_id) const {
        return &col_info_[column_id];
    }

    typedef std::vector<column_info>::const_iterator iterator;
    iterator begin() const {
        return std::begin(col_info_);
    }
    iterator end() const {
        return std::end(col_info_) - hidden_fixed_ - hidden_var_;
    }
    size_t columns_count() const {
        return col_info_.size() - hidden_fixed_ - hidden_var_;
    }
    virtual void freeze() {
        frozen_ = true;
    }

protected:

    int add_hidden_column(const char* name, Value::kind type) {
        verify(!frozen_);
        const bool key = false;     // key: primary index only
        int ret = do_add_column(name, type, key);
        if (type == Value::STR) {
            hidden_var_++;
        } else {
            hidden_fixed_++;
        }
        return ret;
    }

    std::unordered_map<std::string, colid_t> col_name_to_id_;
    std::vector<column_info> col_info_;
    std::vector<colid_t> key_cols_id_;  // key: primary index only

    // number of variable size cols (lookup table on row data)
    int var_size_cols_;
    int fixed_part_size_;

    // number of hidden fixed and var size columns, they are behind visible columns
    int hidden_fixed_;
    int hidden_var_;
    bool frozen_;

private:

    int do_add_column(const char* name, Value::kind type, bool key);
};


class IndexedSchema: public Schema {
    int idx_col_;

    std::vector<std::vector<colid_t>> all_idx_;
    std::map<std::string, int> idx_name_;

    void index_sanity_check(const std::vector<colid_t>& idx);

public:
    IndexedSchema(): idx_col_(-1) {}

    int index_column_id() const {
        return idx_col_;
    }

    int add_index(const char* name, const std::vector<colid_t>& idx);
    int add_index_by_column_names(const char* name, const std::vector<std::string>& named_idx);

    int get_index_id(const std::string& name) {
        auto it = idx_name_.find(name);
        verify(it != idx_name_.end());
        return it->second;
    }
    const std::vector<colid_t>& get_index(const std::string& name) {
        return get_index(get_index_id(name));
    }
    const std::vector<colid_t>& get_index(int idx_id) {
        return all_idx_[idx_id];
    }

    virtual void freeze() {
        if (!frozen_) {
            idx_col_ = this->add_hidden_column(".index", Value::I64);
            verify(idx_col_ >= 0);
        }
        Schema::freeze();
    }

    std::vector<std::vector<colid_t>>::const_iterator index_begin() const {
        return all_idx_.begin();
    }

    std::vector<std::vector<colid_t>>::const_iterator index_end() const {
        return all_idx_.end();
    }
};

} // namespace mdb
