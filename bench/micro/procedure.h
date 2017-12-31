#pragma once

#include "coordinator.h"
#include "bench/micro/workload.h"

namespace deptran {

class MicroProcedure : public TxData {

public:

    MicroProcedure();

    virtual void Init(TxRequest &req);

    virtual void InitR(TxRequest &req);

    virtual void InitW(TxRequest &req);

    virtual bool HandleOutput(int pi, int res,
                              map<int32_t, Value> &output) override;

    virtual bool IsReadOnly();

    virtual void Reset() override;

    virtual ~MicroProcedure();

};

} // namespace deptran
