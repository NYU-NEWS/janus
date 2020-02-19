//
// Created by chance_Lv on 2020/2/17.
//
#pragma once
#include <unordered_map>
#include <deptran/constants.h>
#include <memdb/utils.h>

namespace janus {
    class AccColumn;
    struct MetaData {  // the info returned from rows for consistency check
        // for now, it needs one ssid from each row
        std::unordered_map<mdb::colid_t, snapshotid_t> ssid_highs; // the set of ssid_high of each accessed col
                                                                   // used for later validation
        snapshotid_t highest_ssid_low = 0;
        snapshotid_t lowest_ssid_high = UINT_MAX;
        snapshotid_t highest_ssid_high = 0;
        bool validate_abort = false;  // if any read encounters an unfinalized write and later validation is required
                                      // then no need to validate cuz validation would fail anyway
    };

    class SafeGuard {
        // each txn has a safeguard object for checking consistency
    public:
        bool is_consistent() const;  // compare ssids to tell consistency
    private:
        MetaData metadata;
        friend class AccTxn;
        friend class SchedulerAcc;
    };
}
