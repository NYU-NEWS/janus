//
// Created by chance_Lv on 2020/2/5.
//
#pragma once

//#include "linked_vector.h"
#include "constants.h"
#include <unordered_map>
#include <memdb/value.h>
#include <memdb/row.h>
#include <memdb/utils.h>
/*
 * Defines the row structure in ACCESS.
 * A row is a map of txn_queue (linked_vector); a txn_queue for each col
 */
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
        const mdb::Value& get_value() {
            return value;
        }
    };

    class AccRow : public mdb::Row {
    public:
        static AccRow* create(const mdb::Schema *schema, std::vector<mdb::Value>& values);
        bool read_column(mdb::colid_t col_id, mdb::Value* value);
        bool write_column(mdb::colid_t col_id, mdb::Value&& value, txnid_t tid);
    private:
        // a map of txn_q; keys are cols, values are linkedvectors that holding txns (versions)
        std::unordered_map<mdb::colid_t, std::vector<AccTxnRec>> txn_queue;


    };

}   // namespace janus
