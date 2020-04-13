//
// Created by chance_Lv on 2020/3/29.
//
// control parameters for ML engines
#pragma once

/* server-side ML engine */
#define N_READS (20)  // # of past read arrivals in a feature set
#define N_WRITES (20) // # of past write arrivals
#define N_REQUESTS (20) // # of past requests, for the try of separating read/write models
#define label_t int    // the type of labels
#define NONBLOCK (0)   // as "false" in ml doc
#define BLOCK (1)      // as "true" in ml doc
#define TRAINING_SIZE (10000)  // the number of feature sets for training
//#define TRAINING_INTERVAL (300)  // timer for migrating feature sets to training samples (in seconds)
#define TRAINING_INTERVAL (0)

#define LABELING_TIMER (5)   // the ground truth should be available before these seconds
#define PREDICTION_BAR (0.5) // greater than 0.5 we say it's true

// implementation-related control knobs
#define READ_ARRIVALS_SIZE (10000)  // initial size of READ_ARRIVALS
#define WRITE_ARRIVALS_SIZE (10)    // initial size of WRITE_ARRIVALS
#define INITIAL_N_KEYS (10000)      // for initialize data structures

/* macros used in the cocde */
#define REQUEST_ARRIVALS std::unordered_map<int32_t, std::vector<uint64_t>>
#define WRITE_ARRIVALS std::unordered_map<int32_t, std::vector<uint64_t>>
#define FEATURE_VECTOR std::unordered_map<int32_t, std::set<Features>>
//#define TRAINING_VECTOR std::vector<Features>
#define TRAINING_VECTOR std::vector<std::string>   // VW takes strings as input
#define TRAINING_TIMERS std::unordered_map<int32_t, uint64_t>

/* feature index for read-write together */
#define READ_ARRIVAL_BEGIN (1)
#define WRITE_ARRIVAL_BEGIN (N_READS + READ_ARRIVAL_BEGIN)
#define KEY_POS (WRITE_ARRIVAL_BEGIN + N_WRITES)
#define SSID_POS (KEY_POS + 1)
#define TYPE_POS (SSID_POS + 1)

/* index for separating reads and writes */
// writes
#define REQ_ARRIVAL_BEGIN (1)
#define SEP_KEY_POS (REQ_ARRIVAL_BEGIN + N_REQUESTS)
#define SEP_SSID_POS (SEP_KEY_POS + 1)
#define SEP_TYPE_POS (SEP_SSID_POS + 1)
// reads
#define SEP_WRITE_ARRIVAL_BEGIN (1)
#define SEP_FT_TIME_POS (SEP_TYPE_POS + 1)


/* VW engine parameters */
#define VW_INITIALIZE_STRING ("-b 10 -l 10 -c --passes 25")


/* Client-side ssid predictor macro (heuristics and ML) */
#define TIME_MAP std::unordered_map<parid_t, uint64_t>  // a map of parid_t to physical time delta