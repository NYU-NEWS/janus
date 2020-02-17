//
// Created by chance_Lv on 2020/2/5.
//
#pragma once

#include <memdb/value.h>
#include <deptran/tx.h>
#include "constants.h"
#include "row.h"
#include "safeguard.h"

namespace janus {
    class AccTxn : public Tx {
        using Tx::Tx;
    public:
        /* leave it as is for now, should pass in a const Value*& to avoid copy */
        bool ReadColumn(Row *row,
                        colid_t col_id,
                        Value* value,
                        int hint_flag) override;

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
//        txnid_t tid;
        SafeGuard sg;
    };

}   // namespace janus

