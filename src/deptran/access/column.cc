#include "column.h"

namespace janus {
    AccColumn::AccColumn() {
        txn_queue.reserve(INITIAL_QUEUE_SIZE);
    }

    AccColumn::AccColumn(mdb::Value&& v) {
        verify(txn_queue.empty());  // this should be a create
        txn_queue.emplace_back(std::move(v), 0, FINALIZED);
    }

    //void AccColumn::create(mdb::Value&& v) {
    //    verify(txn_queue.empty());  // this should be a create
    //    txn_queue.emplace_back(std::move(v), 0, FINALIZED);
    //}

    const mdb::Value& AccColumn::read() const {
        // TODO: fill in the read logic, now it returns the last element
        return txn_queue.back().get_value();
    }

    void AccColumn::write(mdb::Value&& v, txnid_t tid) {
        txn_queue.emplace_back(std::move(v), tid);
    }
}
