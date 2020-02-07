#include "row.h"

namespace janus {
    AccRow* AccRow::create(const mdb::Schema *schema,
                           std::vector<mdb::Value>& values) {
        for (mdb::colid_t col_id = 0; col_id < values.size(); ++col_id) {
            // write values to each col in order
            txn_queue.emplace(col_id, AccTxnRec(std::move(values.at(col_id))));
        }
        return this;
    }
}