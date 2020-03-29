#include <chrono>
#include "predictor.h"

namespace janus {
    bool Predictor::should_block(int32_t key, uint64_t arrival_time, snapshotid_t ssid_spec) {
        // TODO: fill the logic, record arrival time and ssid of this new tx
        // append arrival_time to corresponding arrival times
        return false;
    }

    uint64_t Predictor::get_current_time() {
        auto now = std::chrono::high_resolution_clock::now();
        return std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
    }
}
