#pragma once

#include "coordinator.h"
#include "workload.h"

namespace janus {
    class FBChopper: public TxData {
    public:
        FBChopper() = default;
        void Init(TxRequest &req) override;
        bool HandleOutput(int pi,
                          int res,
                          map<int32_t, Value> &output) override { return false;}
        bool IsReadOnly() override;
        void Reset() override;
        ~FBChopper() override = default;

        void FBRotxnInit(TxRequest &req);
        void FBWriteInit(TxRequest &req);
        void FBRotxnRetry();
        void FBWriteRetry();
    };
} // namespace janus
