//
// Created by chance_Lv on 2020/2/5.
//
#pragma once

#include <memdb/value.h>
#include "constants.h"

namespace janus {
    struct AccTxnRec {
        // an access txn record stored in txn queue.
        // This is the element of a node in LinkedVector
        // Each txn record must be a write
        txnid_t txn_id;
        snapshotid_t ssid;      // snapshot id
        acc_status_t status;    // either unchecked or finalized
        mdb::Value value;       // the value of this write

        AccTxnRec() : txn_id(-1), ssid(-1), status(UNCHECKED) {}
    };

}   // namespace janus

