#include "sharding.h"
#include "deptran/frame.h"
#include "deptran/scheduler.h"
#include "parameters.h"

namespace janus {
    int DynamicSharding::PopulateTable(tb_info_t *tb_info_ptr, parid_t par_id) {
        mdb::Table *const table_ptr = tx_sched_->get_table(tb_info_ptr->tb_name);
        const mdb::Schema *schema = table_ptr->schema();
        Value key_value, max_key;
        auto col_it = schema->begin();
        unsigned int col_index = 0;
        for (; col_index < tb_info_ptr->columns.size(); col_index++) {
            verify(col_it != schema->end());
            verify(tb_info_ptr->columns[col_index].name == col_it->name);
            verify(tb_info_ptr->columns[col_index].type == col_it->type);
            verify(tb_info_ptr->columns.size() == 2);  // for now dynamic workload only has 2 cols including the key col.
            if (tb_info_ptr->columns[col_index].is_primary) {
                verify(col_it->indexed);
                verify(tb_info_ptr->columns[col_index].type == Value::I32);  // we use userId as primary key
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
            int n_col = COL_ID;
            verify(n_col > 0);
            for (int index = 0; index <= n_col; ++index) { // we have to populate the key column
                if (tb_info_ptr->columns[index].is_primary) {
                    // this col is the key
                    row_data.push_back(key_value);
                } else {
                    // this col is a regular col
                    if (DYNAMIC_VALUE.get_kind() == mdb::Value::UNKNOWN) {
                        DYNAMIC_VALUE = initialize_dynamic_value();
                    }
                    Value v = DYNAMIC_VALUE;
                    row_data.emplace_back(v);
                }
            }
            verify(row_data.size() == n_col + 1); // we populate n + 1 cols b/c the first col is the primary key
            auto r = frame_->CreateRow(schema, row_data);
            table_ptr->insert(r);
        }
        return 0;
    }
} // namespace janus
