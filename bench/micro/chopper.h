#pragma once

#include "coordinator.h"

namespace deptran {

class MicroBenchChopper : public TxnChopper {

public:

    MicroBenchChopper();

    virtual void init(TxnRequest &req);

    virtual void init_R(TxnRequest &req);

    virtual void init_W(TxnRequest &req);

    virtual bool start_callback(const std::vector<int> &pi, int res, BatchStartArgsHelper &bsah);

    virtual bool start_callback(int pi, int res, const std::vector<mdb::Value> &output);

    virtual bool is_read_only();

    virtual void retry();

    virtual ~MicroBenchChopper();

};

} // namespace deptran
