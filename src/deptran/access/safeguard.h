//
// Created by chance_Lv on 2020/2/17.
//
#pragma once
#include <unordered_map>
#include <deptran/constants.h>
#include <memdb/utils.h>
#include <memdb/row.h>

namespace janus {
    class AccColumn;
    class SSID;
    struct MetaData {  // the info returned from rows for consistency check
        // for now, it needs one ssid from each row
        std::unordered_map<mdb::Row*, std::unordered_map<mdb::colid_t, unsigned long>> indices;
                                                            // the set of ssid_high of each accessed col
                                                            // used for later validation
        //std::unordered_map<mdb::Row*, std::unordered_map<mdb::colid_t, SSID>> ssid_accessed;
                                                            // when read/write a column, record it
                                                            // this is to optimize TPCA-alike workloads
        //std::unordered_map<mdb::Row*, std::unordered_map<mdb::colid_t, unsigned long>> pending_writes;
                                                            // this records all writes, for finalize them later

        snapshotid_t highest_ssid_low = 0;
        snapshotid_t lowest_ssid_high = UINT_MAX;
        snapshotid_t highest_ssid_high = 0;
        bool validate_abort = false;  // if any read encounters an unfinalized write and later validation is required
                                      // then no need to validate cuz validation would fail anyway
    };

    class SafeGuard {
        // each txn has a safeguard object for checking consistency
    public:
        void update_metadata(snapshotid_t ssid_low, snapshotid_t ssid_high);
        void reset_safeguard();
    private:
        MetaData metadata;
        bool validate_done = false;
        friend class AccTxn;
        friend class SchedulerAcc;
    };
}
