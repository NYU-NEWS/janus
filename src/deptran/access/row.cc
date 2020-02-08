#include "row.h"

namespace janus {
    AccRow* AccRow::create(const mdb::Schema *schema,
                           std::vector<mdb::Value>&& values) {
        for (mdb::colid_t col_id = 0; col_id < values.size(); ++col_id) {
            // write values to each col in order
            txn_queue.emplace(col_id, AccTxnRec(std::move(values.at(col_id))));
        }
        return this;
    }

    bool AccRow::read_column(mdb::colid_t col_id, const mdb::Value*& value) {
        // TODO: change this logic, now it reads back() for testing
        if (col_id >= txn_queue.size()) {
            // col_id is invalid. We're doing a trick here.
            // We should make txn_queue check if col_id exists, which
            // is linear time. Instead, we do size() since keys are col_ids
            value = nullptr;
            return false;
        }
        value = txn_queue.at(col_id).back().get_element().get_value();
        return true;
    }
}