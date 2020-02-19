#include "safeguard.h"

namespace janus {
    bool SafeGuard::is_consistent() const {
        // exists an overlapped snapshot range
        return metadata.highest_ssid_low <= metadata.lowest_ssid_high;
    }
}