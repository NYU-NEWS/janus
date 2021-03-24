#include "safeguard.h"

namespace janus {
    void SafeGuard::update_metadata(snapshotid_t ssid_low, snapshotid_t ssid_high, bool is_read) {
        if (ssid_low > metadata.highest_ssid_low) {
            metadata.highest_ssid_low = ssid_low;
        }
        if (ssid_high < metadata.lowest_ssid_high) {
            metadata.lowest_ssid_high = ssid_high;
        }
        /*
        if (is_read) {
            return;
        }
        */
        // only update highest_write_ssid upon writes, so ssids wont increase too quickly
        if (ssid_high > metadata.highest_write_ssid) {
            metadata.highest_write_ssid = ssid_high;
        }
    }

    void SafeGuard::reset_safeguard() {
        metadata.indices.clear();
        metadata.highest_ssid_low = 0;
        metadata.lowest_ssid_high = UINT64_MAX;
        metadata.highest_write_ssid = 0;
        validate_done = false;
        // offset_safe = true;
        ssid_spec = 0;
        decided = true;
        mark_finalized = false;
//        status_query_done = false;
//        status_abort = false;
//        _n_query = 0;
//        _n_query_callback = 0;
	    metadata.reads_for_query.clear();
	    metadata.writes_for_query.clear();
    }
}
