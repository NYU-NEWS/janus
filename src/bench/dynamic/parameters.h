//
// Created by chance_Lv on 2020/3/21.
//
#pragma once
#include "deptran/config.h"

namespace janus {
#define COL_ID 1  // for now each key has only 1 column, which is col_id = 1, col value sizes vary
    static Value initialize_dynamic_value() {
        int value_size = Config::GetConfig()->value_size_;   // in bytes
        std::string value(value_size, '*');
        Value v(value);
        return v;
    }
static Value DYNAMIC_VALUE;
}


