#pragma once

#include "coordinator.h"
#include "workload.h"

namespace janus {
    class SpannerChopper: public TxData {
    public:
        SpannerChopper() = default;
        void Init(TxRequest &req) override;
        bool HandleOutput(int pi,
                          int res,
                          map<int32_t, Value> &output) override { return false;}
        bool IsReadOnly() override;
        void Reset() override;
        ~SpannerChopper() override = default;

        void SpannerRotxnInit(TxRequest &req);
        void SpannerRWInit(TxRequest &req);
        void SpannerRotxnRetry();
        void SpannerRWRetry();
    };
} // namespace janus
