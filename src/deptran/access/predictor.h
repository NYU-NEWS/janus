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
        std::pair<uint, uint> last_n_reads = {0, 0};
        std::pair<uint, uint> last_n_writes = {0, 0};
        int32_t key_ = 0;
        snapshotid_t ssid_spec_ = 0;
        optype_t op_type_ = UNDEFINED;     // either READ_REQ if this piece only has reads or WRITE_REQ if it has any write or UNDEFINED
        // helper funcs
        Features(uint read_low, uint read_high, uint write_low, uint write_high, int32_t key, snapshotid_t ssid_spec, optype_t op_type)
            : last_n_reads(read_low, read_high), last_n_writes(write_low, write_high), key_(key), ssid_spec_(ssid_spec), op_type_(op_type) {}
        Features(Features&& that) noexcept
            : last_n_reads(std::move(that.last_n_reads)),
              last_n_writes(std::move(that.last_n_writes)),
              key_(that.key_),
              ssid_spec_(that.ssid_spec_),
              op_type_(that.op_type_) {}
        Features& operator=(Features&& that) noexcept {
            assert(this != &that);
            this->last_n_reads = std::move(that.last_n_reads);
            this->last_n_writes = std::move(that.last_n_writes);
            this->key_ = that.key_;
            this->ssid_spec_ = that.ssid_spec_;
            this->op_type_ = that.op_type_;
            return *this;
        }
        Features(const Features& that) = default;
        Features& operator=(const Features& that) = delete;
        bool operator<(const Features& that) const {
            return this->ssid_spec_ < that.ssid_spec_;
        }
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
        static READ_ARRIVALS read_arrivals;
        static WRITE_ARRIVALS write_arrivals;
        static FEATURE_VECTOR feature_vector; // a vector of features cross keys so far, periodically moved to learner
        static Features construct_features(int32_t key, snapshotid_t ssid_spec, optype_t op_type);
    };
}