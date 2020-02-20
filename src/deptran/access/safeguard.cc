#include "safeguard.h"

namespace janus {
    bool SafeGuard::is_consistent() const {
        // exists an overlapped snapshot range
        return metadata.highest_ssid_low <= metadata.lowest_ssid_high;
    }

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
}