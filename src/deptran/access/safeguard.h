//
// Created by chance_Lv on 2020/2/17.
//
#pragma once
#include <unordered_map>
#include <deptran/constants.h>
#include <memdb/utils.h>
#include <memdb/row.h>

namespace janus {
    struct MetaData {  // the info returned from rows for consistency check
        // for now, it needs one ssid from each row
        std::unordered_map<mdb::Row*, std::unordered_map<mdb::colid_t, unsigned long>> indices;
                                                            // the set of versions accessed,
                                                            // used for validation and finalize
	    std::unordered_map<mdb::Row*, std::unordered_map<mdb::colid_t, unsigned long>> reads_for_query;
        std::unordered_map<mdb::Row*, std::unordered_map<mdb::colid_t, long>> writes_for_query;
        snapshotid_t highest_ssid_low = 0;
        snapshotid_t lowest_ssid_high = UINT64_MAX;
        snapshotid_t highest_write_ssid = 0;
    };

    class SafeGuard {
        // each txn has a safeguard object for checking consistency
    public:
        void update_metadata(snapshotid_t ssid_low, snapshotid_t ssid_high, bool is_read = true);
        void reset_safeguard();
    private:
        MetaData metadata;
        bool mark_finalized = false;
        snapshotid_t ssid_spec = 0;  // provided by ML on the client for this txn
        bool validate_done = false;
        // bool offset_safe = true;
        bool decided = true;         // if the write a read returning is finalized, used in both dispatch and validate
        // AccStatusQuery related
        bool status_query_done = false;  // for AccStatusQuery
        bool status_abort = false;       // statusquery result is abort
        int _n_query = 0;                // number of watching version
        int _n_query_callback = 0;       // number of decided version
        friend class AccTxn;
        friend class SchedulerAcc;
    };
}
