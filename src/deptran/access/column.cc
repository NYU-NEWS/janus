#include "column.h"
#include "ssid_predictor.h"

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

    const mdb::Value& AccColumn::read(txnid_t tid, snapshotid_t ssid_spec, SSID& ssid, unsigned long& index, bool& decided, bool is_rotxn, bool& rotxn_safe, uint64_t safe_ts, bool& early_abort) {
        // return logical head
        index = _logical_head;
        // early abort for non-rotxn
        if (!is_rotxn && ssid_spec <= txn_queue[index].ssid.ssid_low && txn_queue[index].status != FINALIZED && txn_queue[index].txn_id != tid) {
            // when these conditions met, we need to early abort
            early_abort = true;
            return txn_queue[index].value; // any value, doesnt matter
        }
        if (is_rotxn) {
            do {
                // snapshotid_t low = txn_queue[index].ssid.ssid_low;
                // snapshotid_t ssid_new = low < ssid_spec ? ssid_spec : low + 1;
                // txn_queue[index].extend_ssid(ssid_new);
                if (/*is_rotxn &&*/ txn_queue[index].status != FINALIZED && txn_queue[index].status != ABORTED) {
                    // this version has not been finalized, async wait
                    /*
                    Log_info("rotxn id = %lu; waiting on version by write txn id = %lu; colid = %d; index = %lu, status = %d.",
                             tid, txn_queue[index].txn_id, col_id, index, txn_queue[index].status);
                    */
                    txn_queue[index].status_resolved.Wait([](int val)->bool {
                        return (val == FINALIZED || val == ABORTED); // val is set in AccFinalize
                    });
                    /*
                    Log_info("rotxn id = %lu; waiting_done on version by write txn id = %lu; colid = %d; index = %lu, status = %d.",
                             tid, txn_queue[index].txn_id, col_id, index, txn_queue[index].status);
                    */
                }
                if (txn_queue[index].status == ABORTED) {
                    index = find_prev_version(index);
                    if (txn_queue[index].status == FINALIZED) {
                        // snapshotid_t low = txn_queue[index].ssid.ssid_low;
                        // snapshotid_t ssid_new = low < ssid_spec ? ssid_spec : low + 1;
                        // txn_queue[index].extend_ssid(ssid_new);
                    }
                }
            } while (is_rotxn && txn_queue[index].status != FINALIZED && txn_queue[index].status != ABORTED);
        } else {
            do {
                snapshotid_t ssid_new = read_get_next_SSID(ssid_spec);
                if (ssid_new != ssid_spec && txn_queue[index].status != FINALIZED && txn_queue[index].txn_id != tid) {
                // if (ssid_new != ssid_spec) {  // read has to enable early abort to avoid deadlock!
                    early_abort = true;
                    //Log_info("tid = %lu; col_id = %d; EARLY ABORT.", tid, col_id);
                    return txn_queue[_logical_head].value;  // dummy return, todo: fix this later
                }
                if (!head_not_resolved()) {
                    break;
                }
                // wait
                // Log_info("tid = %lu; col_id = %d; START READ_WAITING, on index = %lu, tx = %lu.", tid, col_id, _logical_head,
                //          txn_queue[_logical_head].txn_id);
                txn_queue[index].status_resolved.Wait([](int val)->bool {
                    return (val == FINALIZED || val == ABORTED); // val is set in AccFinalize
                });
                index = _logical_head;
                // Log_info("tid = %lu; col_id = %d; READ out of wait on index = %lu.", tid, col_id, _logical_head);
                // wait done, event fires
                // update_logical_head();  // might have a new head now after waiting fires, todo: should not need this, done in write
                //Log_info("tid = %lu; col_id = %d; After update_logical_head, head = %lu.", tid, col_id, _logical_head);
            } while (head_not_resolved());
        }

        verify(/*!is_rotxn ||*/ txn_queue[index].status == FINALIZED);
        snapshotid_t low = txn_queue[index].ssid.ssid_low;
        snapshotid_t ssid_new = low < ssid_spec ? ssid_spec : low + 1;
        txn_queue[index].extend_ssid(ssid_new);
        if (is_rotxn && txn_queue[index].write_ts > safe_ts) {
            rotxn_safe = false;
        }
        if (!is_rotxn) {
            if (logical_head_status() != FINALIZED) { // then if this tx turns out consistent and all parts decided, can respond immediately
                verify(0);
                decided = false;
            }
            txn_queue[index].pending_reads.insert(tid);
        }
        ssid = txn_queue[index].ssid;
        return txn_queue[index].value;
    }

    SSID AccColumn::write(mdb::Value&& v, snapshotid_t ssid_spec, txnid_t tid, unsigned long& ver_index,
            bool& decided, unsigned long& prev_index, bool& same_tx, bool& early_abort, bool mark_finalized) { // ver_index is the new write's
        // ---fix after workloads memory efficiency optimization---
        if (txn_queue[_logical_head].txn_id == tid) {
            txn_queue[_logical_head].value = std::move(v);
            same_tx = true;
            return txn_queue[_logical_head].ssid;
        }
        // -----------

        // write returns new ssid
        prev_index = _logical_head;

        // early abort
        if (!mark_finalized &&
                ((ssid_spec <= txn_queue[prev_index].ssid.ssid_low && txn_queue[prev_index].status != FINALIZED) ||
                        (ssid_spec < txn_queue[prev_index].ssid.ssid_high &&
                                (txn_queue[prev_index].pending_reads.size() > 1 ||
                                        (txn_queue[prev_index].pending_reads.size() == 1 &&
                                txn_queue[prev_index].pending_reads.find(tid) != txn_queue[prev_index].pending_reads.end()))))) {
            early_abort = true;
            return txn_queue[prev_index].ssid;  // return any ssid
        }
        /*
        if (!logical_head_ss()) {
            decided = false;
        }
        */
        SSID new_ssid = write_get_next_SSID(ssid_spec);
        uint64_t current_time = SSIDPredictor::get_current_time();
        if (mark_finalized) { // single_shard_write, can safely mark it finalized now.
            txn_queue.emplace_back(std::move(v), tid, new_ssid, FINALIZED, current_time);
        } else {
            txn_queue.emplace_back(std::move(v), tid, new_ssid, UNCHECKED, current_time);
        }
        ver_index = txn_queue.size() - 1; // record index of this pending write for later validation and finalize
        //Log_info("txnid = %lu; writing to; col_id = %d. index = %d.", tid, col_id, ver_index);
        /*
	    if (!is_offset_safe(new_ssid.ssid_low)) {
            offset_safe = false;
        }
        */
        update_logical_head();

        if (wait_for_ss(tid, prev_index)) {
            // Log_info("txnid = %lu; write waiting on tid = %lu; col_id = %d, version = %d, n_r = %d.",
            //        tid, txn_queue[waiting_index].txn_id, this->col_id, waiting_index, txn_queue[waiting_index].pending_reads.size());
            // Log_info("txnid = %lu; write waiting on tid = %lu; pending_reads are: ", tid, txn_queue[waiting_index].txn_id);
            // for (const auto& r : txn_queue[waiting_index].pending_reads) {
            //     Log_info("txnid = %lu; tid = %lu.", tid, r);
            // }
            txn_queue[prev_index].write_ok.Wait([](int val)->bool {
                return (val == FINALIZED); // val is set in AccFinalize
            });
        }
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

    /*
    bool AccColumn::is_offset_safe(snapshotid_t new_ssid) const {
        if (txn_queue[_logical_head].ssid.ssid_low != new_ssid - 1) {
            return true;
        }
        return logical_head_status() != UNCHECKED;
    }
    */

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

    bool AccColumn::logical_head_ss() const {
        // if it is safe for write to proceed ensuring SS
        return (logical_head_status() != UNCHECKED
                && txn_queue[_logical_head].pending_reads.empty());
                // && txn_queue[_logical_head].n_pending_reads == 0);
    }

    // ---------------------------------
    // ----------- finalize ------------
    void AccColumn::finalize(txnid_t tid, unsigned long index, int8_t decision) {
        // Log_info("txnid = %lu. FINALIZE. col_id = %d; index = %d; decision = %d.", tid, col_id, index, decision);
        txn_queue[index].status_resolved.AccSet(decision);
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
        txn_queue[index].status = FINALIZED;
        // no need to update logical head, done for this tx in dispatch
        update_stable_frontier();
        // todo: notify all txns before the stable frontier are safe and okay to return!
    }

    void AccColumn::abort(unsigned long index) {
        // verify(txn_queue[index].status != FINALIZED);  // could have been early aborted
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

    unsigned long AccColumn::find_prev_version(unsigned long index) {
        for (unsigned long i = index - 1; i >= 0; i--) {
            if (txn_queue[i].status != ABORTED) {
                return i;
            }
        }
        verify(0);
    }

    bool AccColumn::wait_for_ss(txnid_t tid, unsigned long index) {
        txn_queue[index].pending_reads.erase(tid);  // if there was reads from the same txn
        return (txn_queue[index].status == UNCHECKED || !txn_queue[index].pending_reads.empty());
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
