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

    const mdb::Value& AccColumn::read(MetaData& metadata) const {
        metadata.read_ssids[this] = finalized_version.second;
        return txn_queue.at(finalized_version.first).value;
    }

    void AccColumn::write(mdb::Value&& v, txnid_t tid, MetaData& metadata) {
        txn_queue.emplace_back(std::move(v), tid);
        metadata.write_ssids[this] = ssid_cur++;
    }

    void AccColumn::update_finalized() {
        unsigned long index = txn_queue.size() - 1;
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
