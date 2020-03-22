#pragma once

#include "deptran/workload.h"

namespace janus {

extern char FB_TABLE[];

// facebook has read-only txns and simple 1-key write (txn)
#define FB_ROTXN (10)
#define FB_ROTXN_NAME "PAGE_RENDER"
#define FB_WRITE (20)
#define FB_WRITE_NAME "PAGE_UPDATE"

class FBWorkload : public Workload {
public:
    void RegisterPrecedures() override;
    std::mt19937 rand_gen_{};
    map<int32_t, int32_t> key_ids_{};
    FBWorkload(Config* config);
    virtual void GetTxRequest(TxRequest* req, uint32_t cid) override;
private:
    void GetWriteRequest(TxRequest* req, uint32_t cid);
    void GetRotxnRequest(TxRequest* req, uint32_t cid);
    int KeyGenerator() const;
    int GetReadBatchSize() const;
    void GenerateReadKeys(std::unordered_set<int>& keys, int size);
};
} // namespace janus
