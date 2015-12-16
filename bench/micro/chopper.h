#pragma once

#include "coordinator.h"
#include "./bench/micro/piece.h"

namespace deptran {

class MicroBenchChopper : public TxnChopper {

public:

    MicroBenchChopper();

    virtual void init(TxnRequest &req);

    virtual void init_R(TxnRequest &req);

    virtual void init_W(TxnRequest &req);

    virtual bool start_callback(const std::vector<int> &pi, int res, BatchStartArgsHelper &bsah);

    virtual bool start_callback(int pi, int res,
                                map<int32_t, Value> &output);

    virtual bool is_read_only();

    virtual void retry();

    virtual ~MicroBenchChopper();

};

} // namespace deptran
