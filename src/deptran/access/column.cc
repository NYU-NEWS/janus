#include "column.h"

namespace janus {
    AccColumn::AccColumn() {
        txn_queue.reserve(INITIAL_QUEUE_SIZE);
    }

    AccColumn::AccColumn(colid_t cid, mdb::Value&& v) {
        verify(txn_queue.empty());  // this should be a create
        txn_queue.emplace_back(std::move(v), 0, FINALIZED);
        col_id = cid;
        update_finalized();
    }

    const mdb::Value& AccColumn::read(MetaData& metadata) {
        // reads return the ssid of the write it returns
        if (finalized_version.first == txn_queue.size() - 1) {
            // the most recent write is finalized, no pending write
            // this read extends the ssid range
            ssid_cur.ssid_high++;
            finalized_version.second.ssid_high++;
        } else {
            // the finalized is some earlier version, there are pending writes
            metadata.validate_abort = true;
        }
        metadata.ssid_highs[col_id] = finalized_version.second.ssid_high;
        update_metadata(metadata, finalized_version.second);
        return txn_queue.at(finalized_version.first).value;
    }

    void AccColumn::write(mdb::Value&& v, txnid_t tid, MetaData& metadata) {
        // write has to return its own new ssid!
        ssid_cur.ssid_low = ++ssid_cur.ssid_high;   // make new ssid_cur upon new write
        txn_queue.emplace_back(std::move(v), tid, ssid_cur);
        metadata.ssid_highs[col_id] = ssid_cur.ssid_high;
        update_metadata(metadata, ssid_cur);
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
                finalized_version.second = it->ssid;
            }
        }
    }

    void AccColumn::update_metadata(MetaData& metadata, const SSID &ssid) {
        if (ssid.ssid_low > metadata.highest_ssid_low) {
            metadata.highest_ssid_low = ssid.ssid_low;
        }
        if (ssid.ssid_high < metadata.lowest_ssid_high) {
            metadata.lowest_ssid_high = ssid.ssid_high;
        }
        if (ssid.ssid_high > metadata.highest_ssid_high) {
            metadata.highest_ssid_high = ssid.ssid_high;
        }
    }
}
