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
            //new_row->_row.emplace(col_id, AccColumn());
            //new_row->_row.at(col_id).create(std::move(values.at(col_id)));
        }
        return new_row;
    }

    SSID AccRow::read_column(mdb::colid_t col_id, mdb::Value* value, MetaData& metadata) {
        // TODO: change this logic, now it reads back() for testing
        if (col_id >= _row.size()) {
            // col_id is invalid. We're doing a trick here.
            // We should make txn_queue check if col_id exists, which
            // is linear time. Instead, we do size() since keys are col_ids
            value = nullptr;
            return {};
        }
        SSID ssid;
        *value = _row.at(col_id).read(metadata, ssid);
        return ssid;
    }

    SSID AccRow::write_column(mdb::colid_t col_id, mdb::Value&& value, txnid_t tid, MetaData& metadata, unsigned long& ver_index) {
        if (col_id >= _row.size()) {
            return {};
        }
        // push back to the txn queue
        return _row.at(col_id).write(std::move(value), tid, metadata, ver_index);
    }

    bool AccRow::validate(mdb::colid_t col_id, snapshotid_t ssid, snapshotid_t ssid_new) {
        // TODO: handle aborted txn rec (FIXME later)
        if (_row[col_id].ssid_cur.ssid_low > ssid) {
            // there has been new write, validation fails
            return false;
        } else {
            if (_row[col_id].ssid_cur.ssid_high < ssid_new) { // extends SSID range
                _row[col_id].ssid_cur.ssid_high = ssid_new;
            }
            return true;
        }
    }

    void AccRow::finalize(mdb::colid_t col_id, unsigned long ver_index, int8_t decision) {
        _row[col_id].txn_queue[ver_index].status = decision;
        _row[col_id].update_finalized();
    }
}