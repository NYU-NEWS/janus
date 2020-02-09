//
// Created by chance_Lv on 2020/2/5.
//
#pragma once

#include <memdb/value.h>
#include <deptran/classic/tx.h>
#include "constants.h"
#include "row.h"

namespace janus {
    class AccTxn : public Tx {
    public:
        // hides the CreateRow virtual function in Tx
        static mdb::Row* CreateRow(const mdb::Schema* schema,
                            std::vector<mdb::Value>&& values);

        static bool ReadColumn(Row *row,
                        colid_t col_id,
                        const Value*& value,
                        int hint_flag);

        bool ReadColumns(Row *row,
                         const std::vector<colid_t>& col_ids,
                         std::vector<Value>* values,
                         int hint_flag) override;

        // hides the WriteColumn virtual function in Tx
        static bool WriteColumn(Row *row,
                         colid_t col_id,
                         Value&& value,
                         int hint_flag);

        // hides the WriteColumns virtual function in Tx
        static bool WriteColumns(Row *row,
                          const std::vector<colid_t> &col_ids,
                          std::vector<Value>&& values,
                          int hint_flag);

        //bool InsertRow(Table *tbl, Row *row) override;

        ~AccTxn() override;
    };

}   // namespace janus

