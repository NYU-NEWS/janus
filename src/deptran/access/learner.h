//
// Created by chance_Lv on 2020/3/28.
//
// Learner is the ML training part that feeds training data to the ML model
// and updates the model periodically
#pragma once

#include "ml_parameters.h"
#include "predictor.h"
#include <unordered_map>
//#include <vowpalwabbit/vw.h>  // the VW ML-engine

namespace janus {
    //class Predictor;
    //class Features;
    class Learner {
    public:
        Learner() = delete;
        ~Learner() = delete;
        static void gather_training_samples(int32_t key);
        //static double vw_predict(const Features& ft);
    private:
        static TRAINING_VECTOR training_samples;  // set of feature sets for training and updating the model
        static TRAINING_TIMERS training_timers;  // timer per key
        static std::string to_vw_string(Features*);
        static void feed_training_samples(Features*);
        static uint32_t head;
        static uint32_t get_head();
        static void get_label(std::string& ft_str, Features* ft);
        static void get_read_arrivals(std::string& ft_str, Features* ft);
        static void get_write_arrivals(std::string& ft_str, Features* ft);
        static void get_key(std::string& ft_str, Features* ft);
        static void get_ssid(std::string& ft_str, Features* ft);
        static void get_type(std::string& ft_str, Features* ft);
        // static uint64_t get_arrival_time(Features* ft);
        /* VW engine below */
        //static vw* model;
        //static void vw_train_model();
    };
}

