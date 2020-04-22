#pragma once

#include "deptran/workload.h"

namespace janus {

extern char SPANNER_TABLE[];

// Spanner has read-only txns and multi-key general read-write txns
#define SPANNER_ROTXN (1)
#define SPANNER_ROTXN_NAME "F1_LOAD"
#define SPANNER_RW (2)
#define SPANNER_RW_NAME "F1_UPDATE"
#define SPANNER_ROTXN_SIZE (7000)
#define SPANNER_RW_SIZE (7001)
#define SPANNER_ROTXN_P(I) (1000 + I)  // # of fragments per client to read or write (1 ~ 500), Table 5
#define SPANNER_RW_P(I) (2000 + I)     // same as above (1 ~ 500), Table 5
#define SPANNER_ROTXN_OUTPUT(I) (3000 + I)
#define SPANNER_RW_OUTPUT(I) (6000 + I)
#define SPANNER_ROTXN_KEY(I) (0 + I)   // keys to read in a rotxn
#define SPANNER_RW_KEY(I) (4000 + I)  // keys to read/write in a read-write txn (can be no reads, but at least 1 write)
#define SPANNER_RW_W_COUNT (8000)  // the # of writes in a RW txns.

    class SpannerWorkload : public Workload {
    public:
        void RegisterPrecedures() override;
        std::mt19937 rand_gen_{};
        map<int32_t, int32_t> key_ids_{};
        SpannerWorkload(Config* config);
        void GetTxRequest(TxRequest* req, uint32_t cid) override;
    private:
        void GetRWRequest(TxRequest* req, uint32_t cid);
        void GetRotxnRequest(TxRequest* req, uint32_t cid);
        int KeyGenerator();
        int GetTxnSize();
        void GenerateKeys(std::unordered_set<int>& keys, int size);
        const Value& get_spanner_value();
    };
} // namespace janus
