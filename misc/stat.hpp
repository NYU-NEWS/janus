#pragma once

namespace rrr {

class AvgStat {
public:
    int64_t n_stat_;
    int64_t sum_;
    int64_t avg_;

    AvgStat(): n_stat_(0), sum_(0), avg_(0) {}
    
    void sample(int64_t s) {
        ++n_stat_;
        sum_ += s;
        avg_ = sum_ / n_stat_;
    }

    void clear() {
        n_stat_ = 0;
        sum_ = 0;
        avg_ = 0;
    }

    AvgStat reset() {
        AvgStat stat = *this;
        clear();
        return stat;
    }

    AvgStat peek() {
        return *this;
    }

};

} // namespace rrr
