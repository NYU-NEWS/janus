#include "safeguard.h"

namespace janus {
    void SafeGuard::update_metadata(snapshotid_t ssid_low, snapshotid_t ssid_high) {
        if (ssid_low > metadata.highest_ssid_low) {
            metadata.highest_ssid_low = ssid_low;
        }
        if (ssid_high < metadata.lowest_ssid_high) {
            metadata.lowest_ssid_high = ssid_high;
        }
        if (ssid_high > metadata.highest_ssid_high) {
            metadata.highest_ssid_high = ssid_high;
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