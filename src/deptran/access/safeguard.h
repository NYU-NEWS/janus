//
// Created by chance_Lv on 2020/2/17.
//
#pragma once
#include <unordered_map>
#include <deptran/constants.h>
//#include "column.h"

namespace janus {
    class AccColumn;
    struct MetaData {  // the info returned from rows for consistency check
        // for now, it needs one ssid from each row
        std::unordered_map<const AccColumn*, snapshotid_t> read_ssids;
        std::unordered_map<const AccColumn*, snapshotid_t> write_ssids;
    };

    class SafeGuard {
        // each txn has a safeguard object for checking consistency
    public:
        bool is_consistent() const;  // compare ssids to tell consistency
    private:
        MetaData metadata;
        friend class AccTxn;
    };
}
