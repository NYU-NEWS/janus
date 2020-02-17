#include "safeguard.h"

namespace janus {
    bool SafeGuard::is_consistent() const {
        // go through both read_ssids and write_ssids
        // if those ssids are not the same, then return false
        auto it = metadata.read_ssids.begin();
        snapshotid_t base = it->second;
        while (it != metadata.read_ssids.end()) {
            if (it->second != base) {
                // ssid not the same, sg cannot tell for sure
                return false;
            }
            it++;
        }
        it = metadata.write_ssids.begin();
        while (it != metadata.write_ssids.end()) {
            if (it->second != base) {
                return false;
            }
            it++;
        }
        return true;
    }
}