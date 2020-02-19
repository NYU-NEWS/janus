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
        snapshotid_t ssid_high = acc_row->read_column(col_id, value, sg.metadata);
        row->ref_copy();
        sg.metadata.ssid_highs[row][col_id] = ssid_high;
        return true;
    }

    bool AccTxn::ReadColumns(Row *row,
                             const std::vector<colid_t> &col_ids,
                             std::vector<Value>* values,
                             int hint_flag) {
        verify(row != nullptr);
        auto acc_row = dynamic_cast<AccRow*>(row);
        row->ref_copy();
        for (auto col_id : col_ids) {
            Value *v = nullptr;
            snapshotid_t ssid_high = acc_row->read_column(col_id, v, sg.metadata);
            verify(v != nullptr);
            values->push_back(std::move(*v));
            sg.metadata.ssid_highs[row][col_id] = ssid_high;
        }
        return true;
    }

    bool AccTxn::WriteColumn(Row *row,
                             colid_t col_id,
                             Value& value,
                             int hint_flag) {
        verify(row != nullptr);
        auto acc_row = dynamic_cast<AccRow*>(row);
        snapshotid_t ssid_high = acc_row->write_column(col_id, std::move(value), this->tid_, sg.metadata);
        row->ref_copy();
        sg.metadata.ssid_highs[row][col_id] = ssid_high;
        return true;
    }

    bool AccTxn::WriteColumns(Row *row,
                              const std::vector<colid_t> &col_ids,
                              std::vector<Value>& values,
                              int hint_flag) {
        verify(row != nullptr);
        auto acc_row = dynamic_cast<AccRow*>(row);
        int v_counter = 0;
        row->ref_copy();
        for (auto col_id : col_ids) {
            snapshotid_t ssid_high =
                    acc_row->write_column(col_id, std::move(values[v_counter++]), this->tid_, sg.metadata);
            sg.metadata.ssid_highs[row][col_id] = ssid_high;
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
