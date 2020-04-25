#pragma once

#include "deptran/workload.h"

namespace janus {

extern char DYNAMIC_TABLE[];

// dynamic workloads have read-only txns and multi-key general read-write txns (could be write-only)
#define DYNAMIC_ROTXN (20001)
#define DYNAMIC_ROTXN_NAME "dynamic_rotxn"
#define DYNAMIC_RW (20002)
#define DYNAMIC_RW_NAME "dynamic_rw_txn"
#define DYNAMIC_TXN_SIZE (13001)
#define DYNAMIC_ROTXN_P(I) (1000 + I)
#define DYNAMIC_RW_P(I) (3000 + I)
#define DYNAMIC_ROTXN_OUTPUT(I) (5000 + I)
#define DYNAMIC_RW_OUTPUT(I) (7000 + I)
#define DYNAMIC_ROTXN_KEY(I) (9000 + I)
#define DYNAMIC_RW_KEY(I) (11000 + I)
#define DYNAMIC_RW_W_COUNT (13000)

    class DynamicWorkload : public Workload {
    public:
        void RegisterPrecedures() override;
        std::mt19937 rand_gen_{};
        map<int32_t, int32_t> key_ids_{};
        DynamicWorkload(Config* config);
        void GetTxRequest(TxRequest* req, uint32_t cid) override;
    private:
        void GetRWRequest(TxRequest* req, uint32_t cid);
        void GetRotxnRequest(TxRequest* req, uint32_t cid);
        int KeyGenerator();
        int GetTxnSize();
        int GetNumWrites(int rw_size);
        void GenerateKeys(std::unordered_set<int>& keys, int size);
        const Value& get_dynamic_value();
    };
} // namespace janus
