#include <chrono>
#include "predictor.h"

namespace janus {
    bool Predictor::should_block(int32_t key, uint64_t arrival_time, snapshotid_t ssid_spec, optype_t op_type) {
        // TODO: fill the logic, record arrival time and ssid of this new tx
        // append arrival_time to corresponding arrival times
        // Log_info("try. should_block. optype = %d.", op_type);
        return false;
    }

    uint64_t Predictor::get_current_time() {
        auto now = std::chrono::high_resolution_clock::now();
        return std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
    }
}
