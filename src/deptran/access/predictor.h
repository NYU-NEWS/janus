//
// Created by chance_Lv on 2020/3/28.
//
// Predictor is the inference part of the ML engine. It takes into features of each arriving txn
// and makes an inference: a prediction that either blocks the txn (true) or execute it right away (false)
#pragma once

#include <cstdint>
#include <deptran/constants.h>

namespace janus {
    class Predictor {
    public:
        Predictor() = delete;  // making this class static, nobody should instantiate it
        ~Predictor() = delete;

        static bool should_block(int32_t key, uint64_t arrival_time, snapshotid_t ssid_spec);  // take a row key, give a prediction
        static uint64_t get_current_time();     // get current time in ms and convert it to ssid type

    };
}