#include "safeguard.h"

namespace janus {
    void SafeGuard::update_metadata(snapshotid_t ssid) {
        if (ssid > metadata.ssid_max) {
            metadata.ssid_max = ssid;
        }
	    if (ssid < metadata.ssid_min) {
            metadata.ssid_min = ssid;
        }
    }

    void SafeGuard::reset_safeguard() {
        metadata.indices.clear();
        metadata.ssid_max = 0;
        metadata.ssid_min = UINT64_MAX;
        //metadata.validate_abort = false;
        validate_done = false;
        offset_safe = true;
        ssid_spec = 0;
    }
}
