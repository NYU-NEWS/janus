//
// Created by chance_Lv on 2020/3/28.
//
// Predictor is the inference part of the ML engine. It takes into features of each arriving txn
// and makes an inference: a prediction that either blocks the txn (true) or execute it right away (false)
#pragma once

#include <cstdint>
#include <deptran/constants.h>
#include <vector>
#include <unordered_map>
#include <set>
#include "ml_parameters.h"

namespace janus {
    struct Features { // the features each tx uses to predict
        // feature variables
        std::pair<uint32_t, uint32_t> last_n_reads = {0, 0};
        std::pair<uint32_t, uint32_t> last_n_writes = {0, 0};
        int32_t key_ = 0;
        snapshotid_t ssid_spec_ = 0;
        optype_t op_type_ = UNDEFINED;     // either READ_REQ if this piece only has reads or WRITE_REQ if it has any write or UNDEFINED
        uint64_t arrival_time_ = 0;        // this txn's execution (arrival) time
        label_t label_ = NONBLOCK;         // initially labelled false as in ML doc
        /****** helper funcs ******/
        Features() = default;
        Features(uint32_t read_low, uint32_t read_high, uint32_t write_low, uint32_t write_high,
                int32_t key, snapshotid_t ssid_spec, optype_t op_type, uint64_t arrival_time, label_t label = NONBLOCK)
            : last_n_reads(read_low, read_high), last_n_writes(write_low, write_high),
              key_(key), ssid_spec_(ssid_spec), op_type_(op_type), arrival_time_(arrival_time), label_(label) {}
        Features(Features&& that) noexcept
            : last_n_reads(std::move(that.last_n_reads)),
              last_n_writes(std::move(that.last_n_writes)),
              key_(that.key_),
              ssid_spec_(that.ssid_spec_),
              op_type_(that.op_type_),
              arrival_time_(that.arrival_time_),
              label_(that.label_) {}
        Features& operator=(Features&& that) noexcept {
            assert(this != &that);
            this->last_n_reads = std::move(that.last_n_reads);
            this->last_n_writes = std::move(that.last_n_writes);
            this->key_ = that.key_;
            this->ssid_spec_ = that.ssid_spec_;
            this->op_type_ = that.op_type_;
            this->arrival_time_ = that.arrival_time_;
            this->label_ = that.label_;
            return *this;
        }

        Features& operator=(const Features& that) {
            assert(this != &that);
            this->last_n_reads = that.last_n_reads;
            this->last_n_writes = that.last_n_writes;
            this->key_ = that.key_;
            this->ssid_spec_ = that.ssid_spec_;
            this->op_type_ = that.op_type_;
            this->arrival_time_ = that.arrival_time_;
            this->label_ = that.label_;
            return *this;
        }

        Features(const Features& that) {
            last_n_reads = that.last_n_reads;
            last_n_writes = that.last_n_writes;
            key_ = that.key_;
            ssid_spec_ = that.ssid_spec_;
            op_type_ = that.op_type_;
            arrival_time_ = that.arrival_time_;
            label_ = that.label_;
        }

        //Features(const Features& that) = delete;
        //Features& operator=(const Features& that) = delete;
        bool operator<(const Features& that) const {  // feature_vector is sorted based on ssid of each feature
            return this->ssid_spec_ < that.ssid_spec_;
        }
        friend class Learner;
    };
    /*
    struct feature_compare {
        // features in feature_vector are sorted based on ssid
        bool operator()(const Features& ft1, const Features& ft2) {
            return ft1.ssid_spec_ < ft2.ssid_spec_;
        }
    };
    */
    class Predictor {
    public:
        Predictor() = delete;  // making this class static, nobody should instantiate it
        ~Predictor() = delete;

        static bool should_block(int32_t key, uint64_t arrival_time, snapshotid_t ssid_spec, optype_t op_type = UNDEFINED);  // take a row key, give a prediction
        static uint64_t get_current_time();     // get current time in ms and convert it to ssid type
    private:
        //static const uint32_t N_FEATURES = TRAINING_SIZE;
        //static uint64_t last_training_time;  // last time we migrated feature_vector to training_samples
        //static TRAINING_TIMERS training_timers;  // timer per key
        static READ_ARRIVALS read_arrivals;
        static WRITE_ARRIVALS write_arrivals;
        static FEATURE_VECTOR feature_vector; // a vector of features cross keys so far, periodically moved to learner
        //static TRAINING_VECTOR training_samples;  // set of feature sets for training and updating the model
        static bool construct_features(Features& ft, int32_t key, snapshotid_t ssid_spec, optype_t op_type, uint64_t arrival_time);
        static void label_features(int32_t key, snapshotid_t ssid_spec, optype_t op_type);
        static bool is_conflict(optype_t t1, optype_t t2);
        //static void gather_training_samples(int32_t key);
        static uint64_t get_current_time_in_seconds();
        static uint64_t get_ft_time_in_seconds(const Features& ft);
        static void initialize_containers();
        static bool initialized;
        friend class Learner;
    };
}