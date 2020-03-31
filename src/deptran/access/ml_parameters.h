//
// Created by chance_Lv on 2020/3/29.
//
// control parameters for ML engines
#pragma once

/* server-side ML engine */
#define N_READS (100)  // # of past read arrivals in a feature set
#define N_WRITES (100) // # of past write arrivals
#define label_t int    // the type of labels
#define NONBLOCK (0)   // as "false" in ml doc
#define BLOCK (1)      // as "true" in ml doc
#define TRAINING_SIZE (1000000)  // the number of feature sets for training
#define TRAINING_INTERVAL (300)  // timer for migrating feature sets to training samples (in seconds)
#define LABELING_TIMER (5)   // the ground truth should be available before these seconds

// implementation-related control knobs
#define READ_ARRIVALS_SIZE (10000)  // initial size of READ_ARRIVALS
#define WRITE_ARRIVALS_SIZE (10)    // initial size of WRITE_ARRIVALS

/* macros used in the cocde */
#define READ_ARRIVALS std::unordered_map<int32_t, std::vector<uint64_t>>
#define WRITE_ARRIVALS std::unordered_map<int32_t, std::vector<uint64_t>>
#define FEATURE_VECTOR std::unordered_map<int32_t, std::set<Features>>
#define TRAINING_VECTOR std::vector<Features>
#define TRAINING_TIMERS std::unordered_map<int32_t, uint64_t>