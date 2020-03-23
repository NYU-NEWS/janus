#include "sharding.h"
#include "deptran/frame.h"
#include "deptran/scheduler.h"
#include "parameters.h"

namespace janus {
    int FBSharding::PopulateTable(tb_info_t *tb_info_ptr, parid_t par_id) {
        mdb::Table *const table_ptr = tx_sched_->get_table(tb_info_ptr->tb_name);
        const mdb::Schema *schema = table_ptr->schema();
        Value key_value, max_key;
        auto col_it = schema->begin();
        unsigned int col_index = 0;
        for (; col_index < tb_info_ptr->columns.size(); col_index++) {
            verify(col_it != schema->end());
            verify(tb_info_ptr->columns[col_index].name == col_it->name);
            verify(tb_info_ptr->columns[col_index].type == col_it->type);
            if (tb_info_ptr->columns[col_index].is_primary) {
                verify(col_it->indexed);
                verify(tb_info_ptr->columns[col_index].type == Value::I64);  // we use userId as primary key
                key_value = value_get_zero(tb_info_ptr->columns[col_index].type);
                max_key = value_get_n(tb_info_ptr->columns[col_index].type,
                            tb_info_ptr->num_records);
            }
            col_it++;
        }
        std::vector<Value> row_data;
        for (; key_value < max_key; ++key_value) {
            // we only populate keys that belong to this svr.
            // TODO: this part of code in tpca has a bug, let shuai know
            if (par_id != PartitionFromKey(key_value, tb_info_ptr)) { continue; }
            row_data.clear();
            // we only populate a subset of columns for each key based on FB stats
            verify(!COL_COUNTS.empty());
            verify(!FB_VALUES.empty());
            int n_col = get_n_column(key_value.get_i32());
            verify(n_col > 0);
            for (int index = 0; index < n_col; ++index) {
                Value v = get_fb_value();
                row_data.emplace_back(v);
            }
            verify(row_data.size() == n_col);
            auto r = frame_->CreateRow(schema, row_data);
            table_ptr->insert(r);
        }
        return 0;
    }

    int FBSharding::get_n_column(i32 key) const {
        verify(key < COL_COUNTS.size());
        return COL_COUNTS.at(key);
    }

    const Value& FBSharding::get_fb_value() const {
        verify(!FB_VALUES.empty());
        std::uniform_int_distribution<> dis(0, FB_VALUES.size() - 1);
        int index = dis(RAND_FB_VALUES);
        return FB_VALUES.at(index);
    }
} // namespace janus