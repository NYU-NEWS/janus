//
// Created by chance_Lv on 2020/3/29.
//
// control parameters for ML engines
#pragma once

/* server-side ML engine */
#define N_READS (100)  // # of past read arrivals in a feature set
#define N_WRITES (100) // # of past write arrivals





/* macros used in the cocde */
#define READ_ARRIVALS std::unordered_map<int32_t, std::vector<uint64_t>>
#define WRITE_ARRIVALS std::unordered_map<int32_t, std::vector<uint64_t>>
#define FEATURE_VECTOR std::unordered_map<int32_t, std::set<Features>>