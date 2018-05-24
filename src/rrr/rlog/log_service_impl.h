#pragma once

#include <map>
#include <set>
#include <string>

#include "rpc/server.h"
#include "rpc/client.h"

#include "log_service.h"

namespace rlog {

struct log_piece {
    rrr::i64 msg_id;
    rrr::i32 level;
    std::string message;

    bool operator< (const log_piece& another) const {
        return msg_id < another.msg_id;
    }
};

struct agg_qps_record {
    rrr::i64 agg_count;
    double tm;

    bool operator <(const agg_qps_record& another) const {
        return tm < another.tm;
    }
};

class RLogServiceImpl: public RLogService {
public:
    RLogServiceImpl(): last_qps_report_tm_(-1) { }

    void log(const rrr::i32& level, const std::string& source, const rrr::i64& msg_id, const std::string& message);
    void aggregate_qps(const std::string& metric_name, const rrr::i32& increment);

private:
    std::map<std::string, rrr::i64> done_;
    std::map<std::string, std::set<log_piece> > buffer_;
    std::map<std::string, std::list<agg_qps_record> > agg_qps_;

    rrr::Mutex l_;
    rrr::Mutex agg_qps_l_;

    double last_qps_report_tm_;
};

}
