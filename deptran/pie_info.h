#pragma once

#include "all.h"

namespace rococo {

class PieInfo {
public:
    uint64_t pie_id_;
    uint64_t txn_id_;

    bool defer_;

    //std::map<cell_locator, int> opset_; //OP_W, OP_DR, OP_IR

    uint32_t type_;

    inline bool operator <(const PieInfo& rhs) const {
        return pie_id_ < rhs.pie_id_;
    }

    inline uint64_t id() {
        return pie_id_;
    }

    inline void set_id(uint64_t id) {
        pie_id_ = id;
    }

    inline void union_data(const PieInfo &pi) {

    }
};


} // namespace deptran
