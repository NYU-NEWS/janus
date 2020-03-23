#pragma once

#include "deptran/workload.h"

namespace janus {

extern char FB_TABLE[];

// facebook has read-only txns and simple 1-key write (txn)
#define FB_ROTXN (10)
#define FB_ROTXN_NAME "PAGE_RENDER"
#define FB_WRITE (20)
#define FB_WRITE_NAME "PAGE_UPDATE"

#define FB_ROTXN_P(I) (10000 + I)
#define FB_WRITE_P(I) (20000 + I)
#define FB_TABLE_KEY(I) (30000 + I)

#define FB_ROTXN_OUTPUT(I) (40000 + I)

#define FB_REQ_VAR_ID(I) (0 + I)

class FBWorkload : public Workload {
public:
    void RegisterPrecedures() override;
    std::mt19937 rand_gen_{};
    map<int32_t, int32_t> key_ids_{};
    FBWorkload(Config* config);
    void GetTxRequest(TxRequest* req, uint32_t cid) override;

private:
    void GetWriteRequest(TxRequest* req, uint32_t cid);
    void GetRotxnRequest(TxRequest* req, uint32_t cid);
    int KeyGenerator();
    int GetReadBatchSize() const;
    void GenerateReadKeys(std::unordered_set<int>& keys, int size);
    const Value& get_fb_value() const;
};
} // namespace janus
