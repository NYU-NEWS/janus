#include "column.h"

namespace janus {
    // ---- basic AccColumn member -----
    AccColumn::AccColumn() {
        txn_queue.reserve(INITIAL_QUEUE_SIZE);
    }

    AccColumn::AccColumn(AccColumn &&other) noexcept
        : col_id(other.col_id),
          _logical_head(other._logical_head),
          _stable_frontier(other._stable_frontier),
          txn_queue(std::move(other.txn_queue)) { }

    AccColumn::AccColumn(mdb::colid_t cid, mdb::Value&& v) {
        verify(txn_queue.empty());  // this should be a create
        txn_queue.emplace_back(std::move(v), 0, FINALIZED);
        col_id = cid;
        update_logical_head(); // reads now return the logical head instead of most recent finalized
        update_stable_frontier();
    }

    const mdb::Value& AccColumn::read(snapshotid_t ssid_spec, SSID& ssid, bool& offset_safe, unsigned long& index, bool& decided, bool& abort) {
        verify(logical_head_status() != ABORTED);
        // wait on logical_head to finalize or abort if needed
        do {
            snapshotid_t ssid_new = read_get_next_SSID(ssid_spec);
            if (ssid_new != ssid_spec) {  // read has to enable early abort to avoid deadlock!
                abort = true;
                return txn_queue[_logical_head].value;  // dummy return, todo: fix this later
            }
            if (!head_not_resolved()) {
                break;
            }
            // wait
            txn_queue[_logical_head].status_resolved.Wait([](int val)->bool {
                return (val == FINALIZED || val == ABORTED); // val is set in AccFinalize
            });
            // wait done, event fires
            update_logical_head();  // might have a new head now after waiting fires
        } while (head_not_resolved());
        // now preceding write (logical head) has been resolved
        verify(logical_head_status() == FINALIZED);
        // return logical head
        index = _logical_head;
        snapshotid_t ssid_new = read_get_next_SSID(ssid_spec);
        // no need to early abort since reading a finalized write even if bumped, use validation later
        // if reads are processed in ssid order, then no need for validation even.
        // extends head's ssid
        txn_queue[index].extend_ssid(ssid_new);
        ssid = txn_queue[index].ssid;
	    if (!is_offset_safe(ssid_new)) {
            offset_safe = false;
        }
        return txn_queue[_logical_head].value;
    }

    SSID AccColumn::write(mdb::Value&& v, snapshotid_t ssid_spec, txnid_t tid, unsigned long& ver_index,
            bool& offset_safe, bool& abort, bool disable_early_abort, bool mark_finalized) { // ver_index is the new write's
        // write returns new ssid
        SSID new_ssid = write_get_next_SSID(ssid_spec);
        if (new_ssid.ssid_low != ssid_spec && !disable_early_abort) {
            abort = true;
            return new_ssid;
        }
        if (mark_finalized) { // single_shard_write, can safely mark it finalized now.
            txn_queue.emplace_back(std::move(v), tid, new_ssid, FINALIZED);
        } else {
            txn_queue.emplace_back(std::move(v), tid, new_ssid);
        }
        ver_index = txn_queue.size() - 1; // record index of this pending write for later validation and finalize
	    if (!is_offset_safe(new_ssid.ssid_low)) {
            offset_safe = false;
        }
        update_logical_head();
        return new_ssid;  // return the SSID of this new write -- safety
    }

    SSID AccColumn::write_get_next_SSID(snapshotid_t ssid_spec) const {
        snapshotid_t new_ssid = logical_head_ssid_for_writes() <= ssid_spec ? ssid_spec
                                : logical_head_ssid_for_writes() + 1;
        return SSID(new_ssid, new_ssid);
    }

    snapshotid_t AccColumn::read_get_next_SSID(snapshotid_t ssid_spec) const {
        return logical_head_ssid_for_reads() < ssid_spec ? ssid_spec : logical_head_ssid_for_reads() + 1;
    }

    bool AccColumn::is_offset_safe(snapshotid_t new_ssid) const {
        if (txn_queue[_logical_head].ssid.ssid_low != new_ssid - 1) {
            return true;
        }
        return logical_head_status() != UNCHECKED;
    }

    bool AccColumn::is_read(txnid_t tid, unsigned long index) const {
        if (index == 0) { // against the default value
            return true;
        } else {
            return tid != txn_queue[index].txn_id;
        }
    }
    // ---------------------------------
    // --------- logical head ----------
    void AccColumn::update_logical_head() {
        _logical_head = logical_head_index();
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

    snapshotid_t AccColumn::logical_head_ssid_for_writes() const {
        return txn_queue[_logical_head].ssid.ssid_high;
    }

    snapshotid_t AccColumn::logical_head_ssid_for_reads() const {
        return txn_queue[_logical_head].ssid.ssid_low;
    }

    acc_status_t AccColumn::logical_head_status() const {
        return txn_queue[_logical_head].status;
    }

    bool AccColumn::is_logical_head(unsigned long index) const {
        return index == _logical_head;
    }

    snapshotid_t AccColumn::next_record_ssid(unsigned long index) const {
        for (unsigned long i = index + 1; i < txn_queue.size(); ++i) {
            if (txn_queue[i].status != ABORTED) {
                return txn_queue[i].ssid.ssid_low;
            }
        }
        return UINT64_MAX;
    }
    // ---------------------------------
    // ----------- finalize ------------
    void AccColumn::finalize(unsigned long index, int8_t decision) {
        txn_queue[index].status_resolved.Set(reinterpret_cast<int &>(decision));  // mark this write has been finalized.
        switch (decision) {
            case FINALIZED:
                commit(index);
                break;
            case ABORTED:
                abort(index);
                break;
            default:
                verify(0);
                break;
        }
    }

    void AccColumn::commit(unsigned long index) {
        verify(txn_queue[index].status == UNCHECKED || txn_queue[index].status == VALIDATING);
        txn_queue[index].status = FINALIZED;
        // no need to update logical head, done for this tx in dispatch
        update_stable_frontier();
        // todo: notify all txns before the stable frontier are safe and okay to return!
    }

    void AccColumn::abort(unsigned long index) {
        verify(txn_queue[index].status != FINALIZED);  // could have been early aborted
        txn_queue[index].status = ABORTED;
        update_logical_head();
        // no need to update stable frontier, wont change anyway upon aborts
        // todo: cascading aborts!
    }

    void AccColumn::update_stable_frontier() {
        unsigned long index = _stable_frontier + 1;
        for (; index < txn_queue.size(); ++index) {
            if (txn_queue[index].status == UNCHECKED || txn_queue[index].status == VALIDATING) {
                break;
            }
        }
        _stable_frontier = index - 1;
    }

    bool AccColumn::head_not_resolved() const {
        return txn_queue[_logical_head].status != FINALIZED && txn_queue[_logical_head].status != ABORTED;
    }

    //----------------------------------------
    //---------------obsolete-----------------
    /*
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

    const AccTxnRec &AccColumn::logical_head() const {
        return txn_queue[_logical_head];
    }
    */
}
