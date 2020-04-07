//
// Created by chance_Lv on 2020/4/6.
//
// Heuristics for client to guess a ssid_spec

#pragma once

#include "ml_parameters.h"
#include "constants.h"
#include <unordered_map>
#include <vector>

namespace janus {
    class SSIDPredictor {
    public:
        SSIDPredictor() = delete;
        ~SSIDPredictor() = delete;
        static void update_time_tracker(parid_t server, uint64_t time);
        static uint64_t predict_ssid(const std::vector<parid_t>& servers);
        static uint64_t get_current_time();  // get client current physical time in micro-seconds

    private:
        static TIME_MAP server_time_tracker;
        static bool INITIALIZED;
        static const uint8_t N_SERVERS = 128;
    };
}
