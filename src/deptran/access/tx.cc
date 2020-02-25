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
        unsigned long index = 0;
        SSID ssid = acc_row->read_column(col_id, value, sg.metadata.validate_abort, index);
        row->ref_copy();
        sg.metadata.indices[row][col_id] = index;  // for later validation
	sg.metadata.earlier_read[row][col_id] = ssid.ssid_high;  // for wild card optimization, i.e., read ssid [0, x]
        // update metadata
	if (ssid.ssid_low == 0) {
            // wild card
            sg.update_metadata(ssid.ssid_low, UINT64_MAX, ssid.ssid_high);
        } else {
            sg.update_metadata(ssid.ssid_low, ssid.ssid_high, ssid.ssid_high);
        }
        sg.offset_1_valid = !sg.metadata.validate_abort;  // offset-1 case
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
            unsigned long index = 0;
            SSID ssid = acc_row->read_column(col_id, v, sg.metadata.validate_abort, index);
            verify(v != nullptr);
            values->push_back(std::move(*v));
            sg.metadata.indices[row][col_id] = index;  // for later validation
	    sg.metadata.earlier_read[row][col_id] = ssid.ssid_high;  // for wild card optimization, i.e., read ssid [0, x]
            // update metadata
	    if (ssid.ssid_low == 0) {
                // wild card
                sg.update_metadata(ssid.ssid_low, UINT64_MAX, ssid.ssid_high);
            } else {
                sg.update_metadata(ssid.ssid_low, ssid.ssid_high, ssid.ssid_high);
            }
            sg.offset_1_valid = !sg.metadata.validate_abort;  // offset-1 case
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
        SSID ssid = acc_row->write_column(col_id, std::move(value), this->tid_, ver_index, sg.offset_1_valid); // ver_index is new write
        row->ref_copy();
	// check wild card optimization
        snapshotid_t ssid_high_check = ssid.ssid_high;
        if (sg.metadata.earlier_read[row].find(col_id) != sg.metadata.earlier_read[row].end()) {
            // write to the same col that this txn earlier read
            ssid_high_check = sg.metadata.earlier_read[row][col_id];
        }
        sg.metadata.indices[row][col_id] = ver_index; // for validation and finalize
	sg.update_metadata(ssid.ssid_low, ssid_high_check, ssid.ssid_high);
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
            SSID ssid = acc_row->write_column(col_id, std::move(values[v_counter++]), this->tid_, ver_index, sg.offset_1_valid);
	    // check wild card optimization
            snapshotid_t ssid_high_check = ssid.ssid_high;
            if (sg.metadata.earlier_read[row].find(col_id) != sg.metadata.earlier_read[row].end()) {
                // write to the same col that this txn earlier read
                ssid_high_check = sg.metadata.earlier_read[row][col_id];
            }
            sg.metadata.indices[row][col_id] = ver_index; // for validation and finalize
            sg.update_metadata(ssid.ssid_low, ssid_high_check, ssid.ssid_high);
        }
        return true;
    }

    AccTxn::~AccTxn() {
        // TODO: fill in if holding any resources
    }
}
