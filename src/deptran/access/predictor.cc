#include <chrono>
#include <rrr/base/all.hpp>
#include "predictor.h"
#include "learner.h"

namespace janus {
    // define static members
    READ_ARRIVALS Predictor::read_arrivals;
    WRITE_ARRIVALS Predictor::write_arrivals;
    FEATURE_VECTOR Predictor::feature_vector;
    //TRAINING_VECTOR Predictor::training_samples;
    //TRAINING_TIMERS Predictor::training_timers;
    bool Predictor::initialized = false;
    //uint64_t Predictor::last_training_time = 0;

    bool Predictor::should_block(int32_t key, uint64_t arrival_time, snapshotid_t ssid_spec, optype_t op_type) {
        initialize_containers();
        Features ft;
        bool features_complete = construct_features(ft, key, ssid_spec, op_type, arrival_time);
        // append arrival_time to corresponding arrival times
        switch (op_type) {
            case READ_REQ:
                if (read_arrivals[key].empty()) {
                    read_arrivals[key].reserve(READ_ARRIVALS_SIZE);
                }
                read_arrivals[key].emplace_back(arrival_time);
                break;
            case WRITE_REQ:
                if (write_arrivals[key].empty()) {
                    write_arrivals[key].reserve(WRITE_ARRIVALS_SIZE);
                }
                write_arrivals[key].emplace_back(arrival_time);
                break;
            case UNDEFINED:
                Log_info("op_type is undefined. Should define it in the workload txn_reg.");
            default: verify(0);
                break;
        }
        // NOTE: we need to make a hard copy of ft to feed to vw for prediction!
        Features ft_copy = ft;
        // insert the feature of this tx to feature_vector
        auto ret = feature_vector[key].insert(std::move(ft));
        // labeling with this new tx. *IMPORTANT* must label after inserting it into vector
        label_features(key, ssid_spec, op_type);
        // check if training interval timer fires, if so migrate feature_vector to training set
        Learner::gather_training_samples(key);
        // todo: query the ML model via VW with ft, and get a prediction
        //double prediction = Learner::vw_predict(*ret.first);
        //double prediction = Learner::vw_predict(ft_copy);
        //Log_debug("Get prediction: %f.", prediction);
        //return prediction > PREDICTION_BAR;  // should block if prediction close to 1
        return false;
    }

    uint64_t Predictor::get_current_time() {
        auto now = std::chrono::system_clock::now();
        return std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
    }

    bool Predictor::construct_features(Features& ft, int32_t key, snapshotid_t ssid_spec, optype_t op_type, uint64_t arrival_time) {
        bool feature_complete = true;
        uint32_t read_low, read_high, write_low, write_high;
        read_high = read_arrivals[key].size() - 1;
        read_low = read_arrivals[key].size() < N_READS ? 0 : read_high - N_READS + 1;
        if (read_arrivals[key].empty()) {  // mark empty by low > high
            read_low = 1;
            read_high = 0;
            feature_complete = false;
        }
        write_high = write_arrivals[key].size() - 1;
        write_low = write_arrivals[key].size() < N_WRITES ? 0 : write_high - N_WRITES + 1;
        if (write_arrivals[key].empty()) {
            write_low = 1;
            write_high = 0;
            feature_complete = false;
        }
        ft = {read_low, read_high, write_low, write_high, key, ssid_spec, op_type, arrival_time};
        return feature_complete;
    }

    void Predictor::label_features(int32_t key, snapshotid_t ssid_spec, optype_t op_type) {
        // note that, feature_vector[key] is an ordered key ordered by ssid, new tx is not necessarily the last element.
        auto rit = feature_vector.at(key).rbegin();
        // feature_vector set is sorted by ssid in ascending order, we iterate reversely
        for (; rit != feature_vector.at(key).rend(); rit++) {
            if (rit->ssid_spec_ > ssid_spec && is_conflict(op_type, rit->op_type_)) {
                // we label this feature "should block"
                auto* ft = const_cast<Features*>(&*rit);  // this is an ugly trick for updating elements in
                                                          // sorted containers. It *should* be safe here b/c
                                                          // we are not modifying the sorting keys of the set
                ft->label_ = BLOCK;
            }
            if (rit->ssid_spec_ <= ssid_spec) {
                // we have some false negatives here by doing <=, but it's fine for now
                break;
            }
        }
    }

    bool Predictor::is_conflict(optype_t t1, optype_t t2) {
        verify(t1 != UNDEFINED && t2 != UNDEFINED);
        return ((t1 == WRITE_REQ && t2 == WRITE_REQ) || t1 != t2);  // true if at least one is write
    }

    /*
    void Predictor::gather_training_samples(int32_t key) {
        auto current_time = get_current_time_in_seconds();
        if (training_timers.find(key) == training_timers.end()) {
            // we have not trained anything for this key yet
            training_timers[key] = current_time;
            return;
        }
        auto last_training_time = training_timers.at(key);
        if (current_time - last_training_time <= TRAINING_INTERVAL) {
            // TODO: should I do this like in a bigger chunk or go through features in every run like GC?
            // TODO: make adjustment based on expt perf
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
    */

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

    void Predictor::initialize_containers() {
        if (initialized) {
            return;
        }
        /*
        if (training_timers.empty()) {
            training_timers.reserve(INITIAL_N_KEYS);
        }
        */
        if (read_arrivals.empty()) {
            read_arrivals.reserve(INITIAL_N_KEYS);
        }
        if (write_arrivals.empty()) {
            write_arrivals.reserve(INITIAL_N_KEYS);
        }
        if (feature_vector.empty()) {
            feature_vector.reserve(INITIAL_N_KEYS);
        }
        /*
        if (training_samples.empty()) { // reserve space for training samples
            training_samples.reserve(TRAINING_SIZE);
        }
        */
        initialized = true;
    }
}
