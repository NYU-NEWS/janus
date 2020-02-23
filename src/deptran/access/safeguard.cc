#include "safeguard.h"

namespace janus {
    void SafeGuard::update_metadata(snapshotid_t ssid_low, snapshotid_t ssid_high) {
        if (ssid_low > metadata.highest_ssid_low) {
            metadata.highest_ssid_low = ssid_low;
        }
        if (ssid_high < metadata.lowest_ssid_high) {
            metadata.lowest_ssid_high = ssid_high;
        }
        // the reason for ssid_high+1 here is: ssid is the preceding write for the new read/write, then
        // the new write's ssid is (ssid_high+1, ssid_high+1).
        if (ssid_high + 1 > metadata.highest_ssid_high) {
            metadata.highest_ssid_high = ssid_high + 1;
        }
    }

    void SafeGuard::reset_safeguard() {
        metadata.indices.clear();
        metadata.highest_ssid_low = 0;
        metadata.lowest_ssid_high = UINT_MAX;
        metadata.highest_ssid_high = 0;
        metadata.validate_abort = false;
        validate_done = false;
        offset_1_valid = true;
    }
}