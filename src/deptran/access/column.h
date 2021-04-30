//
// Created by chance_Lv on 2020/2/14.
//
#pragma once

#include "constants.h"
#include <memdb/utils.h>
#include <memdb/value.h>

namespace janus {
    struct SSID {  // SSID defines an ssid range by low--high
        snapshotid_t ssid_low;   // the ssid of this write upon creation
        snapshotid_t ssid_high;  // the highest ssid extended by reads returning this write
        SSID() : ssid_low(0), ssid_high(0) {}
        SSID(snapshotid_t low, snapshotid_t high) : ssid_low(low), ssid_high(high) {}
        SSID(const SSID& that) = default;
        SSID& operator=(const SSID& that) {
            if (this == &that) {
                return *this;
            }
            this->ssid_low = that.ssid_low;
            this->ssid_high = that.ssid_high;
            return *this;
        }
    };
    struct AccTxnRec {
        // an access txn record stored in txn queue.
        // This is the element of a node in LinkedVector
        // Each txn record must be a write
        txnid_t txn_id;
        SSID ssid;              // snapshot id
        acc_status_t status;    // either unchecked, validating, or finalized
        mdb::Value value;       // the value of this write
        // uint32_t n_pending_reads = 0; // # reads that returning this write
        std::unordered_set<txnid_t> pending_reads = {};
        rrr::SharedIntEvent status_resolved;  // rotxn waiting
        uint64_t write_ts = 0;  // for rotxn check
        rrr::SharedIntEvent write_ok; // for ss

        // rrr::IntEvent ss_safe;  // fires if status is resolved and n_pending_reads = 0
        // rrr::SharedIntEvent status_ready;  // for read Accquery
        std::function<void()> ss_safe = nullptr;

        AccTxnRec() : txn_id(-1), ssid(), status(UNCHECKED), write_ts(0) {}
        explicit AccTxnRec(mdb::Value&& v, txnid_t tid = 0, acc_status_t stat = UNCHECKED)
                : txn_id(tid), ssid(), status(stat), value(std::move(v)), write_ts(0) {
                    status_resolved.AccSet(stat);
                    write_ok.AccSet(ABORTED);
                    // ss_safe.Set(status != UNCHECKED && n_pending_reads == 0);
                    // status_ready.AccSet(stat);
                }
        AccTxnRec(AccTxnRec&& that) noexcept
                : txn_id(that.txn_id),
                  ssid(that.ssid),
                  status(that.status),
                  value(std::move(that.value)),
                  //n_pending_reads(that.n_pending_reads),
                  pending_reads(std::move(that.pending_reads)),
                  status_resolved(std::move(that.status_resolved)),
                  write_ts(that.write_ts),
                  ss_safe(std::move(that.ss_safe)),
                  write_ok(std::move(that.write_ok)){}
                  // ss_safe(std::move(that.ss_safe)),
                  // status_ready(std::move(that.status_ready)) {}
        AccTxnRec(const AccTxnRec&) = delete;
        AccTxnRec& operator=(const AccTxnRec&) = delete;
        AccTxnRec(mdb::Value&& v, txnid_t tid, const SSID& ss_id, acc_status_t stat = UNCHECKED, uint64_t w_ts = 0)
                : txn_id(tid), ssid(ss_id), status(stat), value(std::move(v)), write_ts (w_ts) {}
        void extend_ssid(snapshotid_t ssid_new) {
            if (ssid.ssid_high < ssid_new) { // extend ssid range for reads upon validation
                ssid.ssid_high = ssid_new;
            }
        }
        void update_ssid(snapshotid_t ssid_new) {  // update ssid for write upon validation
            if (ssid.ssid_high < ssid_new) {
                ssid.ssid_low = ssid.ssid_high = ssid_new;
            }
        }
        bool have_reads() const {
            return ssid.ssid_low != ssid.ssid_high;
        }
    };
    // an AccColumn has a vector of queued txns
    class AccColumn {
    public:
        //friend class AccRow;
        AccColumn();
        explicit AccColumn(mdb::colid_t, mdb::Value&& v);
        AccColumn(AccColumn&& that) noexcept;
        AccColumn(const AccColumn&) = delete;   // no copy
        AccColumn& operator=(const AccColumn&) = delete; // no copy
        const mdb::Value& read(txnid_t tid, snapshotid_t ssid_spec, SSID& ssid, unsigned long& index, bool& decided, bool is_rotxn, bool& rotxn_safe, uint64_t safe_ts, bool& early_abort);
        SSID write(mdb::Value&& v, snapshotid_t ssid_spec, txnid_t tid, unsigned long& ver_index,
                bool& decided, unsigned long& prev_index, bool& same_tx, bool& early_abort, bool mark_finalized = false);
    private:
        /* basic column members */
        mdb::colid_t col_id = -1;
        const int INITIAL_QUEUE_SIZE = 100;
        std::vector<AccTxnRec> txn_queue;  // a vector of txns as a versioned column --> txn queue
        SSID write_get_next_SSID(snapshotid_t ssid_spec) const;
        snapshotid_t read_get_next_SSID(snapshotid_t ssid_spec) const;
        // bool is_offset_safe(snapshotid_t new_ssid) const;
        bool is_read(txnid_t tid, unsigned long index) const;
        void finalize(txnid_t tid, unsigned long index, int8_t decision);
        void commit(unsigned long index);
        void abort(unsigned long index);
        unsigned long find_prev_version(unsigned long index);

        /* logical head related */
        unsigned long _logical_head = 0;   // points to logical head, for reads fast reference
        void update_logical_head();
        bool is_logical_head(unsigned long index) const;
        unsigned long logical_head_index() const;
        snapshotid_t logical_head_ssid_for_writes() const;
        snapshotid_t logical_head_ssid_for_reads() const;
        acc_status_t logical_head_status() const;
	    snapshotid_t next_record_ssid(unsigned long index) const;
	    bool logical_head_ss() const;
        bool head_not_resolved() const;

        /* stable frontier related */
        unsigned long _stable_frontier = 0;
        void update_stable_frontier();
        bool wait_for_ss(txnid_t tid, unsigned long index);

        friend class AccRow;
        friend class AccTxn;

        /* ---obsolete----
        SSID ssid_cur;
        unsigned long finalized_version = 0;  // the index points to the most recent finalized version
        unsigned long decided_head = 0;         // before which inclusive all writes are either finalized or aborted
        void update_finalized();    // called whenever commit a write
        void update_decided_head();
        bool all_decided(unsigned long index) const;
        bool all_recent_aborted(unsigned long index) const;
        const AccTxnRec& logical_head() const;
        */
    };
}
