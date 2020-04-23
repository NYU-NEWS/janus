//
// Created by chance_Lv on 2020/3/21.
//
#pragma once

namespace janus {
// spanner workload characteristics defined below
#define SPANNER_WRITE_FRACTION 0.001  // 3 orders of magnitude more rotxns, Table 6
#define MAX_TXN_SIZE 300    // this is the max size of txns.
#define COL_ID 1  // for now each key has only 1 column, which is col_id = 1, col value sizes vary
#define N_SPANNER_VALUES 100 // as used in Eiger (Eiger uses min{50, 10000}, which is too small)

    static std::mt19937 RAND_SPANNER_VALUES(1);
    static std::vector<Value> initialize_spanner_values() {
        double mean = 1.6 * 1024;  // in bytes
        double stddev = 119.0 * 1024;  // in bytes
        double confidence_95 = 1.96;
        uint64_t min = 1;
        uint64_t max = std::round(mean + stddev * confidence_95);
        std::normal_distribution<double> distribution(mean, stddev);
        std::vector<Value> values;
        values.reserve(N_SPANNER_VALUES);
        while (true) {
            double number = distribution(RAND_SPANNER_VALUES);
            if (number > min && number < max) {
                int n_bytes = std::round(number);
                std::string value(n_bytes, '*');
                values.emplace_back(value);
            }
            if (values.size() == N_SPANNER_VALUES) {
                break;
            }
        }
        return values;
    }

static const std::vector<Value> SPANNER_VALUES = initialize_spanner_values();
}


