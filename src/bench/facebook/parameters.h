//
// Created by chance_Lv on 2020/3/21.
//
#pragma once

namespace janus {
// facebook workload characteristics defined below
#define FB_WRITE_FRACTION 0.002
//#define FB_WRITE_FRACTION -1
#define N_KEYS_PER_WRITE 1
#define MAX_TXN_SIZE 1024    // this is the max size of rotxns, FB writes always access 1 key
#define OBJ_FRACTION 0.095   // object to assocs ratio
#define N_CORE_KEYS 10000    // the keys used for storing parameters, real keys are mod n_core_keys
                             // number of total keys is n_friends, which should be larger than n_core_keys
#define N_DISTINCT_VALUES 50 // as used in Eiger (Eiger uses min{50, 10000})

    static std::mt19937 RAND_COL_COUNTS(0);
    static std::mt19937 RAND_FB_VALUES(1);
    static std::mt19937 RAND_FB_KEY_SHUFFLE(2);
    static std::mt19937 RAND_FB_KEY_MAPPING(3);
    static int get_col_count() {
        std::uniform_real_distribution<> dis(0.0, 1.0);
        double token = dis(RAND_COL_COUNTS);
        if (token < 0.709204) { return 1; }
        token -= 0.709204;

        if (token < 0.185909) { return 1; }
        token -= 0.185909;

        if (token < 0.0222263) { return 2; }
        token -= 0.0222263;

        if (token < 0.0202094) { return 4; }
        token -= 0.0202094;

        if (token < 0.017849) { return 8; }
        token -= 0.017849;

        if (token < 0.0150605) { return 16; }
        token -= 0.0150605;

        if (token < 0.0110481) { return 32; }
        token -= 0.0110481;

        if (token < 0.00653806) { return 64; }
        token -= 0.00653806;

        if (token < 0.0080639) { return 128; }
        token -= 0.0080639;

        if (token < 0.00175157) { return 256; }
        token -= 0.00175157;

        if (token < 0.00104441) { return 512; }
        token -= 0.00104441;

        //0.000681581 10 1546
        //0.00024909 11 565
        //8.90552e-05 12 202
        //7.3184e-05 13 166
        //2.20434e-06 14 5
        //we just lump these really unlikely sizes together
        return 1024;
    }

    static std::vector<int> initialize_col_counts() {
        std::vector<int> counts;
        counts.reserve(N_CORE_KEYS);
        for (int i = 0; i < N_CORE_KEYS; ++i) {
            counts.emplace_back(get_col_count());
        }
        return counts;
    }

    static int get_obj_col_size() {
        // return in # of bits
        std::uniform_real_distribution<> dis(0.0, 1.0);
        double token = dis(RAND_FB_VALUES);
        if (token < 0.00704422) { return 1; }
        token -= 0.00704422;

        //0 0 0
        //1 0 0

        if (token < 0.000306422) { return 4; }
        token -= 0.000306422;

        //3 0.00847108 104554
        if (token < 0.00847108) { return 8; }
        token -= 0.00847108;

        //4 0.0389854 481176
        if (token < 0.0389854) { return 16; }
        token -= 0.0389854;

        //5 0.239204 2952363
        if (token < 0.239204) { return 32; }
        token -= 0.239204;

        //6 0.195086 2407841
        if (token < 0.195086) { return 64; }
        token -= 0.195086;

        //7 0.209904 2590727
        if (token < 0.209904) { return 128; }
        token -= 0.209904;

        //8 0.177451 2190177
        if (token < 0.177451) { return 256; }
        token -= 0.177451;

        //9 0.0412025 508540
        if (token < 0.0412025) { return 512; }
        token -= 0.0412025;

        //10 0.0176862 218291
        if (token < 0.0176862) { return 1024; }
        token -= 0.0176862;

        //11 0.0201117 248228
        if (token < 0.0201117) { return 2048; }
        token -= 0.0201117;

        //12 0.0294145 363047
        if (token < 0.0294145) { return 4096; }
        token -= 0.0294145;

        //13 0.00809425 99903
        if (token < 0.00809425) { return 8192; }
        token -= 0.00809425;

        //14 0.00378539 46721
        if (token < 0.00378539) { return 16384; }
        token -= 0.00378539;

        //15 0.0022045 27209
        //16 0.000855583 10560
        //17 0.000162933 2011
        //18 3.12742e-05 386
        //call them all 2^15
        return 32768;
    }

    static int get_assoc_col_size() {
        // return in # of bits
        std::uniform_real_distribution<> dis(0.0, 1.0);
        double token = dis(RAND_FB_VALUES);
        //-1 0.384472 99465600
        if (token < 0.384472) { return 1; }
        token -= 0.384472;

        //0 0 0
        //1 0.100752 26065213
        if (token < 0.100752) { return 2; }
        token -= 0.100752;

        //2 0.0017077 441795
        if (token < 0.0017077) { return 4; }
        token -= 0.0017077;

        //3 0.00769552 1990885
        if (token < 0.00769552) { return 8; }
        token -= 0.00769552;

        //4 0.325119 84110720
        if (token < 0.325119) { return 16; }
        token -= 0.325119;

        //5 0.115914 29987896
        if (token < 0.115914) { return 32; }
        token -= 0.115914;

        //6 0.0365789 9463233
        if (token < 0.0365789) { return 64; }
        token -= 0.0365789;

        //7 0.0139127 3599313
        if (token < 0.0139127) { return 128; }
        token -= 0.0139127;

        //8 0.00197022 509710
        if (token < 0.00197022) { return 256; }
        token -= 0.00197022;

        //9 0.00137401 355467
        if (token < 0.00137401) { return 512; }
        token -= 0.00137401;

        //10 0.000139049 35973
        if (token < 0.000139049) { return 1024; }
        token -= 0.000139049;

        //11 0.000291774 75484
        if (token < 0.000291774) { return 2048; }
        token -= 0.000291774;

        //12 0.00515581 1333846
        if (token < 0.00515581) { return 4096; }
        token -= 0.00515581;

        //13 0.00491133 1270597
        if (token < 0.00491133) { return 8192; }
        token -= 0.00491133;

        //14 5.53908e-06 1433
        if (token < 0.00000553908) { return 16384; }
        token -= 0.00000553908;

        //15 7.73075e-09 2
        return 32768;
    }

    static int get_fb_col_size() {
        // return in # of bits
        std::uniform_real_distribution<> dis(0.0, 1.0);
        double token = dis(RAND_FB_VALUES);
        if (token < OBJ_FRACTION) {
            return get_obj_col_size();
        } else {
            return get_assoc_col_size();
        }
    }

    static std::vector<Value> initialize_fb_values() {
        std::vector<Value> values;
        values.reserve(N_DISTINCT_VALUES);
        for (int i = 0; i < N_DISTINCT_VALUES; ++i) {
            int col_size = get_fb_col_size(); // in bits
            int n_bytes = col_size / 8 == 0 ? 1 : col_size / 8;
            std::string value(n_bytes, '*');
            values.emplace_back(value);
        }
        return values;
    }

    static std::vector<int> initialize_fb_shuffled_keys() {
        std::vector<int> keys(N_CORE_KEYS);
        std::iota( std::begin(keys), std::end(keys), 0 );
        std::shuffle( keys.begin(), keys.end() , RAND_FB_KEY_SHUFFLE);
        return keys;
    }

static const std::vector<int> COL_COUNTS = initialize_col_counts();
static const std::vector<Value> FB_VALUES = initialize_fb_values();
static const std::vector<int> SHUFFLED_KEYS = initialize_fb_shuffled_keys();
}


