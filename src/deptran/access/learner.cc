#include <string>
#include <rrr/base/all.hpp>
#include "learner.h"

namespace janus {
    TRAINING_VECTOR Learner::training_samples;
    TRAINING_TIMERS Learner::training_timers;
    uint32_t Learner::head = 0;
    vw* Learner::model = VW::initialize(VW_INITIALIZE_STRING);

    void Learner::gather_training_samples(int32_t key) {
        if (training_timers.empty()) {
            training_timers.reserve(INITIAL_N_KEYS);
        }
        if (training_samples.empty()) { // reserve space for training samples
            training_samples.reserve(TRAINING_SIZE);
        }
        auto current_time = Predictor::get_current_time_in_seconds();
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
        auto& feature_set = Predictor::feature_vector.at(key);
        auto itr = feature_set.begin();
        for (; itr != feature_set.end(); itr++) {
            if (current_time - Predictor::get_ft_time_in_seconds(*itr) > LABELING_TIMER) {
                // labeling timer fires, this feature is labeled.
                auto* ft = const_cast<Features*>(&*itr);
                feed_training_samples(ft);
            } else {
                break;   // we could have left some fired feature sets not moved, but okay
            }
        }
        feature_set.erase(feature_set.begin(), itr);  // remove those migrated elements from feature_vector
        if (current_time - training_timers.at(key) >= TRAINING_INTERVAL) {
            vw_train_model();
            training_timers.at(key) = current_time;
        }
    }

    std::string &&Learner::to_vw_string(Features* ft) {
        assert(ft != nullptr);
        std::string ft_str;
        get_label(ft_str, ft);
        get_read_arrivals(ft_str, ft);
        get_write_arrivals(ft_str, ft);
        get_key(ft_str, ft);
        get_ssid(ft_str, ft);
        get_type(ft_str, ft);
        return std::move(ft_str);
    }

    void Learner::feed_training_samples(Features* ft) {
        uint32_t pos = get_head();
        if (pos >= training_samples.size()) {
            training_samples.emplace_back(to_vw_string(ft));
        } else {
            training_samples.at(pos) = to_vw_string(ft);
        }
    }

    uint32_t Learner::get_head() {
        uint32_t current_head = head++;
        if (head == training_samples.capacity()) {
            // we keep capacity unchanged, and wrap around to reuse storage
            head = 0;
        }
        return current_head;
    }

    void Learner::get_label(std::string &ft_str, Features* ft) {
        switch (ft->label_) {
            case 0:
                ft_str.push_back('0');
                break;
            case 1:
                ft_str.push_back('1');
                break;
            default: verify(0);
                break;
        }
        ft_str += " | ";
        // e.g., 1 |
    }

    void Learner::get_read_arrivals(std::string &ft_str, Features* ft) {
        if (ft->last_n_reads.first > ft->last_n_reads.second) {
            // empty read arrival features
            return;
        }
        int ft_index = READ_ARRIVAL_BEGIN;
        for (int i = ft->last_n_reads.first; i <= ft->last_n_reads.second; i++, ft_index++) {
            // e.g., 1 | 1:1032334 2:3242342 3:432424
            ft_str += std::to_string(ft_index);
            ft_str.push_back(':');
            ft_str += std::to_string(Predictor::read_arrivals.at(ft->key_).at(i));
            ft_str.push_back(' ');
        }
        verify(ft_index < WRITE_ARRIVAL_BEGIN);
    }

    void Learner::get_write_arrivals(std::string &ft_str, Features *ft) {
        if (ft->last_n_writes.first > ft->last_n_writes.second) {
            // empty write arrival features
            return;
        }
        int ft_index = WRITE_ARRIVAL_BEGIN;
        for (int i = ft->last_n_writes.first; i <= ft->last_n_writes.second; i++, ft_index++) {
            // e.g., 1 | 1:1032334 2:3242342 3:432424 101:32423423 102:123242332
            ft_str += std::to_string(ft_index);
            ft_str.push_back(':');
            ft_str += std::to_string(Predictor::write_arrivals.at(ft->key_).at(i));
            ft_str.push_back(' ');
        }
        verify(ft_index < KEY_POS);
    }

    void Learner::get_key(std::string &ft_str, Features *ft) {
        ft_str += std::to_string(KEY_POS);
        ft_str.push_back(':');
        ft_str += std::to_string(ft->key_);
        ft_str.push_back(' ');
    }

    void Learner::get_ssid(std::string &ft_str, Features *ft) {
        ft_str += std::to_string(SSID_POS);
        ft_str.push_back(':');
        ft_str += std::to_string(ft->ssid_spec_);
        ft_str.push_back(' ');
    }

    void Learner::get_type(std::string &ft_str, Features *ft) {
        ft_str += std::to_string(TYPE_POS);
        ft_str.push_back(':');
        ft_str += std::to_string(ft->op_type_);
    }

    void Learner::vw_train_model() {
        for (const auto& ft_str : training_samples) {
            example *e = VW::read_example(*model, ft_str);
            model->learn(*e);
            VW::finish_example(*model, *e);
        }
    }

    double Learner::vw_predict(const Features& ft) {
        std::string ft_str = to_vw_string(const_cast<Features *>(&ft));
        example *e = VW::read_example(*model, ft_str);
        model->predict(*e);
        VW::finish_example(*model, *e);
        return (double)VW::get_prediction(e);
    }
}