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
        for (mdb::colid_t col_id = 0; col_id < values.size(); ++col_id) {
            // write values to each col in order
            new_row->txn_queue.emplace(col_id, AccTxnRec(std::move(values.at(col_id)), 0, FINALIZED));
        }
        return new_row;
    }

    bool AccRow::read_column(mdb::colid_t col_id, mdb::Value* value) {
        // TODO: change this logic, now it reads back() for testing
        if (col_id >= txn_queue.size()) {
            // col_id is invalid. We're doing a trick here.
            // We should make txn_queue check if col_id exists, which
            // is linear time. Instead, we do size() since keys are col_ids
            value = nullptr;
            return false;
        }
        *value = txn_queue.at(col_id).back().get_element().get_value();
        return true;
    }

    bool AccRow::write_column(mdb::colid_t col_id, mdb::Value&& value, txnid_t tid) {
        if (col_id >= txn_queue.size()) {
            return false;
        }
        // push back to the txn queue
        txn_queue.at(col_id).push_back(std::move(Node<AccTxnRec>(std::move(AccTxnRec(std::move(value), tid)))));
        return true;
    }

}