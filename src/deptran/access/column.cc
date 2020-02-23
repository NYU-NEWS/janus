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

    const mdb::Value& AccColumn::read(bool& validate_abort, SSID& ssid, unsigned long& index) const {
        // return the most recent finalized version
        index = finalized_version;
        ssid = txn_queue[index].ssid;   // TODO:check
        validate_abort = !is_logical_head(index);
        return txn_queue.at(finalized_version).value;
    }

    SSID AccColumn::write(mdb::Value&& v, txnid_t tid, unsigned long& ver_index, bool& is_decided) { // ver_index is the new write's
        // write also returns the ssid of preceding write
        SSID new_ssid = get_next_SSID();
        txn_queue.emplace_back(std::move(v), tid, new_ssid);
        ver_index = txn_queue.size() - 1; // record index of this pending write for later validation and finalize
        if (txn_queue[ver_index - 1].status == UNCHECKED || txn_queue[ver_index - 1].status == VALIDATING) {
            // for check off-1 for writes, only need to check the one preceding write
            is_decided = false;
        }
        return new_ssid;  // return the SSID of this new write -- safety
    }

    void AccColumn::update_finalized() {
        unsigned long index = txn_queue.size() - 1;
        for (auto it = txn_queue.rbegin(); it != txn_queue.rend(); it++, index--) {
            if (it->status == FINALIZED) {
                finalized_version = index;
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

    bool AccColumn::is_logical_head(unsigned long index) const {
        // return true if this index is either the physical back() of the queue
        // or all later indices have been aborted
        return index == txn_queue.size() - 1 || all_recent_aborted(index);
    }

    SSID AccColumn::get_next_SSID() const {
        snapshotid_t low = logical_head().ssid.ssid_high >= txn_queue.back().ssid.ssid_high
                           ? logical_head().ssid.ssid_high + 1
                           : txn_queue.back().ssid.ssid_high + 1;
        return SSID(low, low);
    }

    const AccTxnRec &AccColumn::logical_head() const {
        // logical head could be either finalized_version or a pending write
        for (auto it = txn_queue.rbegin(); it != txn_queue.rend(); it++) {
            if (it->status != ABORTED) {
                return *it;
            }
        }
        verify(0);
        return txn_queue.back();
    }

    void AccColumn::update_decided_head() {
        for (unsigned long x = decided_head + 1; x < txn_queue.size(); ++x) {
            if (txn_queue[x].status == UNCHECKED || txn_queue[x].status == VALIDATING) {
                decided_head = x - 1;
                return;
            }
        }
    }

    bool AccColumn::all_decided(unsigned long index) const {
        return decided_head >= index;   // actually it is strictly =
    }

    unsigned long AccColumn::logical_head_index() const {
        // logical head could be either finalized_version or a pending write
        unsigned long index = txn_queue.size() - 1;
        for (auto it = txn_queue.rbegin(); it != txn_queue.rend(); it++, index--) {
            if (it->status != ABORTED) {
                return index;
            }
        }
        verify(0);
        return 0;
    }
}
