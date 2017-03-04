#pragma once

#include "coordinator.h"
#include "bench/micro/workload.h"

namespace deptran {

class MicroProcedure : public Procedure {

public:

    MicroProcedure();

    virtual void Init(TxnRequest &req);

    virtual void InitR(TxnRequest &req);

    virtual void InitW(TxnRequest &req);

    virtual bool HandleOutput(int pi, int res,
                              map<int32_t, Value> &output) override;

    virtual bool IsReadOnly();

    virtual void Reset() override;

    virtual ~MicroProcedure();

};

} // namespace deptran
