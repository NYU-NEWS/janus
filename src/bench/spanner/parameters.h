//
// Created by chance_Lv on 2020/3/21.
//
#pragma once

namespace janus {
// spanner workload characteristics defined below
#define SPANNER_WRITE_FRACTION 0.003  // rotxn:w/rtxn = 340:1, Table 6
//#define SPANNER_WRITE_FRACTION 1
#define MAX_TXN_SIZE 10    // this is the max size of txns.
#define COL_ID 1  // for now each key has only 1 column, which is col_id = 1, col value sizes vary
#define N_SPANNER_VALUES 50
#define N_CORE_KEYS 10000  // for memory efficiency

    static std::mt19937 RAND_SPANNER_KEY_SHUFFLE(2);
    static std::mt19937 RAND_SPANNER_VALUES(1);
    static std::mt19937 RAND_SPANNER_KEY_MAPPING(3);
    static std::vector<Value> initialize_spanner_values() {
        // double mean = 1.6 * 1024;  // in bytes
        // double stddev = 119.0 * 1024;  // in bytes
        double mean = 1.6;  // in bytes
        double stddev = 119.0;  // in bytes
        double confidence_95 = 1.96;
        uint64_t min = 1;
        uint64_t max = std::round(mean + stddev * confidence_95);
        std::normal_distribution<double> distribution(mean, stddev);
        std::vector<Value> values;
        values.reserve(N_SPANNER_VALUES);
        while (true) {
            double number = distribution(RAND_SPANNER_VALUES);
            if (number >= max) { continue; }
            int n_bytes = number <= min ? 1 : std::round(number);
            std::string value(n_bytes, '*');
            values.emplace_back(value);
            if (values.size() == N_SPANNER_VALUES) {
                break;
            }
        }
        return values;
    }

    static std::vector<int> initialize_spanner_shuffled_keys() {
        std::vector<int> keys(N_CORE_KEYS);
        std::iota( std::begin(keys), std::end(keys), 0 );
        std::shuffle( keys.begin(), keys.end() , RAND_SPANNER_KEY_SHUFFLE);
        return keys;
    }

static const std::vector<Value> SPANNER_VALUES = initialize_spanner_values();
static const std::vector<int> SHUFFLED_KEYS = initialize_spanner_shuffled_keys();
}


