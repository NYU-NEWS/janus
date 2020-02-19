#include "tx.h"

namespace janus {
    // TODO: fill in these stubs
    bool AccTxn::ReadColumn(Row *row,
                            colid_t col_id,
                            Value* value,
                            int hint_flag) {
        verify(row != nullptr);
        // class downcasting to get AccRow
        auto acc_row = dynamic_cast<AccRow*>(row);
        return acc_row->read_column(col_id, value, sg.metadata);
    }

    bool AccTxn::ReadColumns(Row *row,
                             const std::vector<colid_t> &col_ids,
                             std::vector<Value>* values,
                             int hint_flag) {
        verify(row != nullptr);
        auto acc_row = dynamic_cast<AccRow*>(row);
        for (auto col_id : col_ids) {
            Value *v = nullptr;
            if (acc_row->read_column(col_id, v, sg.metadata)) {
                verify(v != nullptr);
                values->push_back(std::move(*v));
            } else {
                return false;
            }
        }
        return true;
    }

    bool AccTxn::WriteColumn(Row *row,
                             colid_t col_id,
                             Value& value,
                             int hint_flag) {
        verify(row != nullptr);
        auto acc_row = dynamic_cast<AccRow*>(row);
        return acc_row->write_column(col_id, std::move(value), this->tid_, sg.metadata);
    }

    bool AccTxn::WriteColumns(Row *row,
                              const std::vector<colid_t> &col_ids,
                              std::vector<Value>& values,
                              int hint_flag) {
        verify(row != nullptr);
        auto acc_row = dynamic_cast<AccRow*>(row);
        int v_counter = 0;
        for (auto col_id : col_ids) {
            if (!acc_row->write_column(col_id, std::move(values[v_counter++]), this->tid_, sg.metadata)) {
                return false;
            }
        }
        return true;
    }

    /*
    bool AccTxn::InsertRow(Table *tbl, Row *row) {
        return false;
    }
    */

    AccTxn::~AccTxn() {
        // TODO: fill in if holding any resources
    }
}
