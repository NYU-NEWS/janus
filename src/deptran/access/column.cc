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

    const mdb::Value& AccColumn::read(bool& validate_abort, SSID& ssid, unsigned long& index) {
        // reads return the ssid of the write it returns for sg check and validation
        /*
        if (finalized_version.first == txn_queue.size() - 1 || all_recent_aborted(finalized_version.first)) {
            // the most recent write is finalized, no pending write
            // this read extends the ssid range
            // ssid_cur.ssid_high++;  // we now do this in AccFinalize
            // finalized_version.second.ssid_high++;
        } else {
            // the finalized is some earlier version, there are pending writes
            validate_abort = true;
        }
        */
        if (finalized_version.first != txn_queue.size() - 1 && !all_recent_aborted(finalized_version.first)) {
            // there are concurrent pending writes
            validate_abort = true;
        }
        //metadata.ssid_highs[col_id] = finalized_version.second.ssid_high;
        //ssid_high = finalized_version.second.ssid_high;
        //update_metadata(metadata, finalized_version.second);
        index = finalized_version.first;
        ssid = finalized_version.second;
        return txn_queue.at(finalized_version.first).value;
    }

    SSID AccColumn::write(mdb::Value&& v, txnid_t tid, unsigned long& ver_index) { // ver_index is the new write's
        // write also returns the ssid of preceding write
        txn_queue.back().ssid = ssid_cur;  // update txn rec's SSID with ssid_cur
        ssid_cur.ssid_low = ++ssid_cur.ssid_high;   // make new ssid_cur upon new write
        txn_queue.emplace_back(std::move(v), tid, ssid_cur);
        //metadata.ssid_highs[col_id] = ssid_cur.ssid_high;
        //update_metadata(metadata, ssid_cur);
        ver_index = txn_queue.size() - 1; // record index of this pending write for later validation and finalize
        return txn_queue[ver_index -1].ssid;  // return the SSID of the preceding write
    }

    void AccColumn::update_finalized() {
        unsigned long index = txn_queue.size() - 1;
        for (auto it = txn_queue.rbegin(); it != txn_queue.rend(); it++, index--) {
            if (it->status == FINALIZED) {
                // even if the finalized version has not been changed, but its SSD might have been extended.
                finalized_version.first = index;
                finalized_version.second = it->ssid;
                return;
            }
        }
    }

    bool AccColumn::all_recent_aborted(unsigned long index) const {
        for (++index; index < txn_queue.size(); ++index) {
            if (txn_queue[index].status != ABORTED) {
                return false;
            }
        }
        return true;
    }

    /*
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
    */
}
