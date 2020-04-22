#pragma once

#include <bench/tpcc_real_dist/sharding.h>

namespace janus {
    class FBSharding: public Sharding {
        int PopulateTable(tb_info_t *tb_info, parid_t) override;
        void PreparePrimaryColumn(tb_info_t *tb_info,
                                  uint32_t col_index,
                                  mdb::Schema::iterator &col_it) override { verify(0); };

        bool GenerateRowData(tb_info_t *tb_info,
                             uint32_t &sid,
                             Value &key_value,
                             vector<Value> &row_data) override { verify(0); };

        void InsertRowData(tb_info_t *tb_info,
                           uint32_t &partition_id,
                           Value &key_value,
                           const mdb::Schema *schema,
                           mdb::Table *const table_ptr,
                           mdb::SortedTable *tbl_sec_ptr,
                           vector<Value> &row_data) override { verify(0); };

        bool Ready2Populate(tb_info_t *tb_info) override { return true; };
        int get_n_column(i32 key) const;
        const Value& get_fb_value() const;
    };
} // namespace janus
