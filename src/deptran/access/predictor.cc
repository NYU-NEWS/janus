#include <chrono>
#include <rrr/base/all.hpp>
#include "predictor.h"

namespace janus {
    // define static members
    READ_ARRIVALS Predictor::read_arrivals;
    WRITE_ARRIVALS Predictor::write_arrivals;
    FEATURE_VECTOR Predictor::feature_vector;
    TRAINING_VECTOR Predictor::training_samples;
    TRAINING_TIMERS Predictor::training_timers;
    //uint64_t Predictor::last_training_time = 0;

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
            default: verify(0);
                break;
        }
        Features ft = construct_features(key, ssid_spec, op_type);
        // insert the feature of this tx to feature_vector
        feature_vector[key].insert(std::move(ft));
        // labeling with this new tx. *IMPORTANT* must label after inserting it into vector
        label_features(key, ssid_spec, op_type);
        // check if training interval timer fires, if so migrate feature_vector to training set
        gather_training_samples(key);
        // todo: query the ML model via VW with ft, and get a prediction
        return false;
    }

    uint64_t Predictor::get_current_time() {
        auto now = std::chrono::system_clock::now();
        return std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
    }

    Features Predictor::construct_features(int32_t key, snapshotid_t ssid_spec, optype_t op_type) {
        uint read_high = read_arrivals.size() - 1;
        uint read_low = read_high - N_READS + 1;
        uint write_high = write_arrivals.size() - 1;
        uint write_low = write_high - N_WRITES + 1;
        return {read_low, read_high, write_low, write_high, key, ssid_spec, op_type};
    }

    void Predictor::label_features(int32_t key, snapshotid_t ssid_spec, optype_t op_type) {
        auto rit = feature_vector.at(key).rbegin();
        rit++;  // we skip the most recently added element, which is this very tx
        // feature_vector set is sorted by ssid in ascending order, we iterate reversely
        for (; rit != feature_vector.at(key).rend(); rit++) {
            if (rit->ssid_spec_ >= ssid_spec && is_conflict(op_type, rit->op_type_)) {
                // we label this feature "should block"
                auto* ft = const_cast<Features*>(&*rit);  // this is an ugly trick for updating elements in
                                                          // sorted containers. It *should* be safe here b/c
                                                          // we are not modifying the sorting keys of the set
                ft->label_ = BLOCK;
            }
            if (rit->ssid_spec_ < ssid_spec) {
                break;
            }
        }
    }

    bool Predictor::is_conflict(optype_t t1, optype_t t2) {
        verify(t1 != UNDEFINED && t2 != UNDEFINED);
        return (t1 == t2 == WRITE_REQ || t1 != t2);  // true if at least one is write
    }

    void Predictor::gather_training_samples(int32_t key) {
        if (training_samples.empty()) { // reserve space for training samples
            training_samples.reserve(TRAINING_SIZE);
        }
        auto current_time = get_current_time_in_seconds();
        if (training_timers.find(key) == training_timers.end()) {
            // we have not trained anything for this key yet
            training_timers[key] = current_time;
            return;
        }
        auto last_training_time = training_timers.at(key);
        if (current_time - last_training_time <= TRAINING_INTERVAL) {
            // timer has not fired yet
            return;
        }
        // o/w we migrate feature vectors to training samples
        auto& feature_set = feature_vector.at(key);
        auto itr = feature_set.begin();
        for (; itr != feature_set.end(); itr++) {
            if (current_time - get_ft_time_in_seconds(*itr) > LABELING_TIMER) {
                // labeling timer fires, this feature is labeled.
                auto* ft = const_cast<Features*>(&*itr);
                training_samples.emplace_back(std::move(*ft));
            } else {
                break;   // we could have left some fired feature sets not moved, but okay
            }
        }
        feature_set.erase(feature_set.begin(), itr);  // remove those migrated elements from feature_vector
        training_timers[key] = current_time;
    }

    uint64_t Predictor::get_current_time_in_seconds() {
        auto now = std::chrono::system_clock::now();
        return std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
    }

    uint64_t Predictor::get_ft_time_in_seconds(const Features &ft) {
        switch (ft.op_type_) {
            case READ_REQ:
                return read_arrivals.at(ft.key_).at(ft.last_n_reads.second) / 1000000;
            case WRITE_REQ:
                return write_arrivals.at(ft.key_).at(ft.last_n_writes.second) / 1000000;
            case UNDEFINED:
                Log_info("op_type is undefined. Should define it in the workload txn_reg.");
            default: verify(0);
                return 0;
        }
    }
}
