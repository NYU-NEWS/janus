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
        SSID ssid = acc_row->read_column(col_id, value, sg.metadata);
        row->ref_copy();
        if (sg.metadata.ssid_accessed[row].find(col_id) == sg.metadata.ssid_accessed[row].end()) {
            // no early write from the same txn at the same row-col
            sg.metadata.ssid_accessed[row][col_id] = ssid;
        }
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
            SSID ssid = acc_row->read_column(col_id, v, sg.metadata);
            verify(v != nullptr);
            values->push_back(std::move(*v));
            if (sg.metadata.ssid_accessed[row].find(col_id) == sg.metadata.ssid_accessed[row].end()) {
                // no early write from the same txn at the same row-col
                sg.metadata.ssid_accessed[row][col_id] = ssid;
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
        unsigned long ver_index = 0;
        SSID ssid = acc_row->write_column(col_id, std::move(value), this->tid_, sg.metadata, ver_index);
        row->ref_copy();
        sg.metadata.ssid_accessed[row][col_id] = ssid; // we insert anyway, possible overwrite earlier reads
        sg.metadata.pending_writes[row][col_id] = ver_index; // for finalize/abort this write later
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
            unsigned long ver_index = 0;
            SSID ssid = acc_row->write_column(col_id, std::move(values[v_counter++]), this->tid_, sg.metadata, ver_index);
            sg.metadata.ssid_accessed[row][col_id] = ssid;
            sg.metadata.pending_writes[row][col_id] = ver_index;
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
