//
// Created by chance_Lv on 2020/3/28.
//
// Predictor is the inference part of the ML engine. It takes into features of each arriving txn
// and makes an inference: a prediction that either blocks the txn (true) or execute it right away (false)
#pragma once

#include <cstdint>
#include <deptran/constants.h>
#include <vector>
#include "row.h"

namespace janus {
    struct FEATURES { // the features each tx uses to predict
        std::pair<int, int> last_n_reads;
        std::pair<int, int> last_n_writes;
        int32_t key;
        snapshotid_t ssid_spec;
        optype_t op_type;     // either READ_REQ if this piece only has reads or WRITE_REQ if it has any write or UNDEFINED
    };
    class Predictor {
    public:
        Predictor() = delete;  // making this class static, nobody should instantiate it
        ~Predictor() = delete;

        static bool should_block(int32_t key, uint64_t arrival_time, snapshotid_t ssid_spec, optype_t op_type = UNDEFINED);  // take a row key, give a prediction
        static uint64_t get_current_time();     // get current time in ms and convert it to ssid type
    private:
        static std::unordered_map<int32_t, std::vector<uint64_t>> read_arrivals;
        static std::unordered_map<int32_t, std::vector<uint64_t>> write_arrivals;
    };
}