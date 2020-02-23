#include <memdb/value.h>
#include "row.h"

namespace janus {
    AccRow* AccRow::create(const mdb::Schema *schema,
                           std::vector<mdb::Value>& values) {
        std::vector<const mdb::Value*> values_ptr(values.size(), nullptr);
        size_t fill_counter = 0;
        for (auto& value : values) {
            fill_values_ptr(schema, values_ptr, value, fill_counter);
            fill_counter++;
        }
        auto* raw_row = new AccRow();
        auto* new_row = (AccRow*)mdb::Row::create(raw_row, schema, values_ptr);
        for (auto col_info_it = schema->begin(); col_info_it != schema->end(); col_info_it++) {
            int col_id = schema->get_column_id(col_info_it->name);
            new_row->_row.emplace(
                    std::piecewise_construct,
                    std::make_tuple(col_id),
                    std::make_tuple(col_id, std::move(values.at(col_id))));
        }
        return new_row;
    }

    SSID AccRow::read_column(mdb::colid_t col_id, mdb::Value* value, bool& validate_abort, unsigned long& index) const {
        if (col_id >= _row.size()) {
            // col_id is invalid. We're doing a trick here.
            // keys are col_ids
            value = nullptr;
            return {};
        }
        SSID ssid;
        *value = _row.at(col_id).read(validate_abort, ssid, index);
        return ssid;
    }

    SSID AccRow::write_column(mdb::colid_t col_id, mdb::Value&& value, txnid_t tid, unsigned long& ver_index, bool& is_decided) {
        if (col_id >= _row.size()) {
            return {};
        }
        // push back to the txn queue
        return _row.at(col_id).write(std::move(value), tid, ver_index, is_decided);
    }

    bool AccRow::validate(mdb::colid_t col_id, unsigned long index, snapshotid_t ssid_new) { // index is the new write's
        if (!_row[col_id].is_logical_head(index)) {
            // there have been recent unaborted writes
            if (_row[col_id].txn_queue[index].status == UNCHECKED) {
                _row[col_id].txn_queue[index].status = ABORTED;  // will abort anyways
                // TODO: abort merge
            }
            return false;
        }
        // now, validation succeeds, i.e., index is logical head, update ssid
        if (_row[col_id].txn_queue[index].ssid.ssid_high < ssid_new) {
            // extends ssid high for either read or write
            _row[col_id].txn_queue[index].ssid.ssid_high = ssid_new;
            if (_row[col_id].txn_queue[index].status == UNCHECKED) {
                // validating a write, at index is an unfinalized write
                _row[col_id].txn_queue[index].status = VALIDATING;  // mark validating
                // update both low and high to new, e.g., from [2,2] to [5,5] for writes
                _row[col_id].txn_queue[index].ssid.ssid_low = ssid_new;
            }
        }
        return true;
    }

    void AccRow::finalize(mdb::colid_t col_id, unsigned long ver_index, int8_t decision, snapshotid_t ssid_new) {
        bool is_read = true;  // the being finalized request is a read, then at ver_index is a finalized write
        // update status
        if (_row[col_id].txn_queue[ver_index].status == UNCHECKED ||
            _row[col_id].txn_queue[ver_index].status == VALIDATING) {
            _row[col_id].txn_queue[ver_index].status = decision;
            is_read = false;
        }
//        _row[col_id].update_decided_head();
        if (decision == ABORTED) {
            // TODO: abort merge
            return;
        }
        // update finalized_version
        _row[col_id].update_finalized();
        // now we update ssid for the special ssid-diff=1 case --> iff ssid_new != 0
        if (ssid_new == 0) {
            return;
        }
        if (_row[col_id].txn_queue[ver_index].ssid.ssid_high < ssid_new) {
            // update ssid, similar to validation
            _row[col_id].txn_queue[ver_index].ssid.ssid_high = ssid_new;
            if (!is_read) {
                // this is a write, bounce up
                _row[col_id].txn_queue[ver_index].ssid.ssid_low = ssid_new;
            }
        }
    }
}