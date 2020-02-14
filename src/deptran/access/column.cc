#include "column.h"

namespace janus {
    AccColumn::AccColumn() {
        txn_queue.reserve(INITIAL_QUEUE_SIZE);
    }

    AccColumn::AccColumn(mdb::Value&& v) {
        verify(txn_queue.empty());  // this should be a create
        txn_queue.emplace_back(std::move(v), 0, FINALIZED);
        update_finalized();
    }

    const mdb::Value& AccColumn::read() const {
        // TODO: fill in the read logic, now it returns the last element
        //return txn_queue.back().get_value();
        return txn_queue.at(finalized_version.first).value;
    }

    void AccColumn::write(mdb::Value&& v, txnid_t tid) {
        txn_queue.emplace_back(std::move(v), tid);
    }

    void AccColumn::update_finalized() {
        int index = txn_queue.size() - 1;
        for (auto it = txn_queue.rbegin(); it != txn_queue.rend(); it++, index--) {
            if (it->status == FINALIZED) {
                if (finalized_version.first == index) {
                    // most recent finalized not changed yet
                    return;
                }
                finalized_version.first = index;
            }
        }
    }
}
