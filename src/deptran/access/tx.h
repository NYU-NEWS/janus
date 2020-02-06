//
// Created by chance_Lv on 2020/2/5.
//
#pragma once

#include <memdb/value.h>
#include <deptran/classic/tx.h>
#include "constants.h"
#include "row.h"

namespace janus {
    class AccTxn : public TxClassic {
    public:
        using TxClassic::TxClassic;

        AccRow* CreateRow(const mdb::Schema *schema,
                          const std::vector<mdb::Value> &values) override;

        bool ReadColumn(Row *row,
                        colid_t col_id,
                        Value *value,
                        int hint_flag) override;

        bool ReadColumns(Row *row,
                         const std::vector<colid_t> &col_ids,
                         std::vector<Value> *values,
                         int hint_flag) override;

        bool WriteColumn(Row *row,
                         colid_t col_id,
                         const Value &value,
                         int hint_flag) override;

        bool WriteColumns(Row *row,
                          const std::vector<colid_t> &col_ids,
                          const std::vector<Value> &values,
                          int hint_flag) override;

        bool InsertRow(Table *tbl, Row *row) override;

        AccRow* Query(mdb::Table *tbl,
                      const mdb::MultiBlob &mb,
                      int64_t row_context_id = 0) override;

        AccRow* Query(mdb::Table *tbl,
                      vector<Value> &primary_keys,
                      int64_t row_context_id = 0) override;

        ~AccTxn() override;
    };

}   // namespace janus

