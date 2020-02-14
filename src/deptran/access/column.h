//
// Created by chance_Lv on 2020/2/14.
//
#pragma once

#include "constants.h"
#include <memdb/value.h>

namespace janus {
    struct AccTxnRec {
        // an access txn record stored in txn queue.
        // This is the element of a node in LinkedVector
        // Each txn record must be a write
        txnid_t txn_id;
        snapshotid_t ssid;      // snapshot id
        acc_status_t status;    // either unchecked, validating, or finalized
        mdb::Value value;       // the value of this write

        AccTxnRec() : txn_id(-1), ssid(-1), status(UNCHECKED) {}
        explicit AccTxnRec(mdb::Value&& v, txnid_t tid = 0, acc_status_t stat = UNCHECKED)
                : txn_id(tid), ssid(0), status(stat), value(std::move(v)) {}
        AccTxnRec(AccTxnRec&& that) noexcept
                : txn_id(that.txn_id),
                  ssid(that.ssid),
                  status(that.status),
                  value(std::move(that.value)) {}
        AccTxnRec(const AccTxnRec&) = delete;
        AccTxnRec& operator=(const AccTxnRec&) = delete;
        AccTxnRec(txnid_t tid, snapshotid_t ss_id, acc_status_t stat, mdb::Value&& v)
                : txn_id(tid), ssid(ss_id), status(stat), value(std::move(v)) {}
        const mdb::Value & get_value() const {
            return value;
        }
    };
    // an AccColumn has a vector of queued txns
    class AccColumn {
    public:
        //friend class AccRow;
        AccColumn();
        explicit AccColumn(mdb::Value&& v);
        AccColumn(const AccColumn&) = delete;   // no copy
        AccColumn& operator=(const AccColumn&) = delete; // no copy
        //void create(mdb::Value&& v);
        const mdb::Value& read() const;
        void write(mdb::Value&& v, txnid_t tid);
    private:
        // a vector of txns as a versioned column --> txn queue
        std::vector<AccTxnRec> txn_queue;
        const int INITIAL_QUEUE_SIZE = 100;
    };
}
