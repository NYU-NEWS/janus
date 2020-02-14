#include <memdb/value.h>
#include "row.h"

namespace janus {
    AccRow* AccRow::create(const mdb::Schema *schema,
                           std::vector<mdb::Value>& values) {
        std::vector<const mdb::Value*> values_ptr(values.size(), nullptr);
        size_t fill_counter = 0;
        for (auto it = values.begin(); it != values.end(); ++it) {
            fill_values_ptr(schema, values_ptr, *it, fill_counter);
            fill_counter++;
        }
        auto* raw_row = new AccRow();
        AccRow* new_row = (AccRow*)mdb::Row::create(raw_row, schema, values_ptr);
        for (auto col_info_it = schema->begin(); col_info_it != schema->end(); col_info_it++) {
            int col_id = schema->get_column_id(col_info_it->name);
            new_row->_row.emplace(
                    std::piecewise_construct,
                    std::make_tuple(col_id),
                    std::make_tuple(std::move(values.at(col_id))));
            //new_row->_row.emplace(col_id, AccColumn());
            //new_row->_row.at(col_id).create(std::move(values.at(col_id)));
        }
        return new_row;
    }

    bool AccRow::read_column(mdb::colid_t col_id, mdb::Value* value) {
        // TODO: change this logic, now it reads back() for testing
        if (col_id >= _row.size()) {
            // col_id is invalid. We're doing a trick here.
            // We should make txn_queue check if col_id exists, which
            // is linear time. Instead, we do size() since keys are col_ids
            value = nullptr;
            return false;
        }
        *value = _row.at(col_id).read();
        return true;
    }

    bool AccRow::write_column(mdb::colid_t col_id, mdb::Value&& value, txnid_t tid) {
        if (col_id >= _row.size()) {
            return false;
        }
        // push back to the txn queue
        _row.at(col_id).write(std::move(value), tid);
        return true;
    }
}