#pragma once

#include "coordinator.h"
#include "workload.h"

namespace janus {
    class DynamicChopper: public TxData {
    public:
        DynamicChopper() = default;
        void Init(TxRequest &req) override;
        bool HandleOutput(int pi,
                          int res,
                          map<int32_t, Value> &output) override { return false;}
        bool IsReadOnly() override;
        void Reset() override;
        ~DynamicChopper() override = default;

        void DynamicRotxnInit(TxRequest &req);
        void DynamicRWInit(TxRequest &req);
        void DynamicRotxnRetry();
        void DynamicRWRetry();
    };
} // namespace janus
