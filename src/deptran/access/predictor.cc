#include <chrono>
#include <rrr/base/all.hpp>
#include "predictor.h"

namespace janus {
    // define static members
    READ_ARRIVALS Predictor::read_arrivals;
    WRITE_ARRIVALS Predictor::write_arrivals;
    FEATURE_VECTOR Predictor::feature_vector;

    bool Predictor::should_block(int32_t key, uint64_t arrival_time, snapshotid_t ssid_spec, optype_t op_type) {
        // append arrival_time to corresponding arrival times
        switch (op_type) {
            case READ_REQ:
                read_arrivals[key].emplace_back(arrival_time);
                break;
            case WRITE_REQ:
                write_arrivals[key].emplace_back(arrival_time);
                break;
            case UNDEFINED:
                Log_info("op_type is undefined. Should define it in the workload txn_reg.");
            default: assert(0);
                break;
        }
        Features ft = construct_features(key, ssid_spec, op_type);
        //feature_vector.emplace(key, std::set<Features>{std::move(ft)}); // put the feature of this new tx to feature_vector
        feature_vector[key].insert(std::move(ft));
        // todo: labeling with this new tx

        // todo: query the ML model via VW with ft, and get a prediction
        return false;
    }

    uint64_t Predictor::get_current_time() {
        auto now = std::chrono::high_resolution_clock::now();
        return std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
    }

    Features Predictor::construct_features(int32_t key, snapshotid_t ssid_spec, optype_t op_type) {
        uint read_high = read_arrivals.size() - 1;
        uint read_low = read_high - N_READS + 1;
        uint write_high = write_arrivals.size() - 1;
        uint write_low = write_high - N_WRITES + 1;
        return {read_low, read_high, write_low, write_high, key, ssid_spec, op_type};
    }
}
