//
// Created by chance_Lv on 2020/2/5.
//
#pragma once

#include "linked_vector.h"
#include "constants.h"
#include <unordered_map>
#include <memdb/value.h>
#include <memdb/row.h>
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
    };

    class AccRow : public mdb::Row {
    public:



    private:
        // a map of txn_q; keys are cols, values are linkedvectors that holding txns (versions)
        std::unordered_map<int, LinkedVector<AccTxnRec>> txn_queue;



    };

}   // namespace janus
