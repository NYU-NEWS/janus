//
// Created by chance_Lv on 2020/2/5.
//
#pragma once

#include <memdb/value.h>
#include <deptran/tx.h>
#include "constants.h"
#include "row.h"

namespace janus {
    class AccTxn : public Tx {
        using Tx::Tx;
    public:
        // hides the CreateRow virtual function in Tx
        //static mdb::Row* CreateRow(const mdb::Schema* schema,
        //                    std::vector<mdb::Value>&& values);

        static bool ReadColumn(Row *row,
                        colid_t col_id,
                        const Value*& value,
                        int hint_flag);

        bool ReadColumns(Row *row,
                         const std::vector<colid_t>& col_ids,
                         std::vector<Value>* values,
                         int hint_flag) override;

        // hides the WriteColumn virtual function in Tx
        bool WriteColumn(Row *row,
                         colid_t col_id,
                         Value& value,
                         int hint_flag) override;

        // hides the WriteColumns virtual function in Tx
        bool WriteColumns(Row *row,
                          const std::vector<colid_t> &col_ids,
                          std::vector<Value>& values,
                          int hint_flag) override;

        //bool InsertRow(Table *tbl, Row *row) override;

        ~AccTxn() override;
    private:
        txnid_t tid;

    };

}   // namespace janus

