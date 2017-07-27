#pragma once

namespace rrr {

class AvgStat {
public:
    int64_t n_stat_;
    int64_t sum_;
    int64_t avg_;
    int64_t max_;
    int64_t min_;

    AvgStat(): n_stat_(0), sum_(0), avg_(0), max_(0), min_(0) {}
    
    void sample(int64_t s = 1) {
        ++n_stat_;
        sum_ += s;
        avg_ = sum_ / n_stat_;
        max_ = s > max_ ? s : max_;
        min_ = s < min_ ? s : min_;
    }

    void clear() {
        n_stat_ = 0;
        sum_ = 0;
        avg_ = 0;
        max_ = 0;
        min_ = 0;
    }

    AvgStat reset() {
        AvgStat stat = *this;
        clear();
        return stat;
    }

    AvgStat peek() {
        return *this;
    }

    int64_t avg() {
        return avg_;
    }
};

} // namespace rrr
