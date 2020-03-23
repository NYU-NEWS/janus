
#include "deptran/__dep__.h"
#include "deptran/config.h"
#include "workload.h"
#include "zipf.h"
#include "parameters.h"

namespace janus {
    char FB_TABLE[] = "friends";
    FBWorkload::FBWorkload(Config* config) : Workload(config) {
        std::map<std::string, uint64_t> table_num_rows;
        sharding_->get_number_rows(table_num_rows);
        fb_para_.n_friends_ = table_num_rows[std::string(FB_TABLE)];
        //verify(single_server_ == Config::SS_DISABLED);
        switch (single_server_) {
            case Config::SS_DISABLED:
                fix_id_ = -1;
                break;
            case Config::SS_THREAD_SINGLE:
            case Config::SS_PROCESS_SINGLE: {
                fix_id_ = RandomGenerator::rand(0, fb_para_.n_friends_ - 1);
                unsigned int f;
                sharding_->GetPartition(FB_TABLE, Value(fix_id_), f);
                break;
            }
            default:
                verify(0);
        }
        rand_gen_.seed((int)std::time(0) + (uint64_t)pthread_self());
    }

    void FBWorkload::GetTxRequest(TxRequest* req, uint32_t cid) {
        // decide this req is rotxn or write
        std::uniform_real_distribution<> dis(0.0, 1.0);
        double token = dis(rand_gen_); // get a token: a random double between 0 and 1
        if (token <= FB_WRITE_FRACTION) { // this is a write
            req->tx_type_ = FB_WRITE;
            GetWriteRequest(req, cid);
        } else { // this is a rotxn
            req->tx_type_ = FB_ROTXN;
            GetRotxnRequest(req, cid);
        }
        req->n_try_ = n_try_;
    }

    void FBWorkload::GetWriteRequest(TxRequest* req, uint32_t cid) {
        for (int i = 0; i < N_KEYS_PER_WRITE; ++i) { // fb should have 1 key per write
            int key = KeyGenerator();
            req->input_[FB_REQ_VAR_ID(i)] = Value(key);
        }
        req->input_[FB_OP_COUNT] = Value(N_KEYS_PER_WRITE);
    }

    void FBWorkload::GetRotxnRequest(TxRequest *req, uint32_t cid) {
        int rotxn_size = GetReadBatchSize();
        std::unordered_set<int> keys;
        GenerateReadKeys(keys, rotxn_size);
        int index = 0;
        for (const auto& key : keys) {
            req->input_[FB_REQ_VAR_ID(index++)] = Value(key);
        }
        req->input_[FB_OP_COUNT] = Value((i32)keys.size());
    }

    int FBWorkload::KeyGenerator() {
        const auto& dist = Config::GetConfig()->dist_;
        if (dist == "uniform") { // uniform workloads
            std::uniform_int_distribution<> d(0, fb_para_.n_friends_ - 1);
            return d(rand_gen_);
        } else if (dist == "zipf") { // skewed workloads
            static auto alpha = Config::GetConfig()->coeffcient_;
            static ZipfDist d(alpha, fb_para_.n_friends_);
            return d(rand_gen_);  // TODO: tpca does "rotate", what is that for?
        } else { // FIXME: we do not support fixed_dist for now
            verify(0); // do not support other workloads for now
            return 0;
        }
    }

    int FBWorkload::GetReadBatchSize() {
        std::uniform_real_distribution<> dis(0.0, 1.0);
        double token = dis(rand_gen_);
        // the following logic is gotten from Eiger's FB workload generator (TAO)
        //0 = .515;
        if (token < .515) { return 1; }
        token -= .515;

        //1 = .1;
        if (token < .1) { return 2; }
        token -= .1;

        //2 = .12;
        if (token < .12) { return 4; }
        token -= .12;

        //3 = .1;
        if (token < .1) { return 8; }
        token -= .1;

        //4 = .08;
        if (token < .08) { return 16; }
        token -= .08;

        //5 = .05;
        if (token < .05) { return 32; }
        token -= .05;

        //6 = .02;
        if (token < .02) { return 64; }
        token -= .02;

        //7 = .008;
        if (token < .008) { return 128; }
        token -= .008;

        //8 = .004;
        if (token < .004) { return 256; }
        token -= .004;

        //9 = .002;
        if (token < .002) { return 512; }
        token -= .002;

        //+ = .001;
        return 1024;
    }

    void FBWorkload::GenerateReadKeys(std::unordered_set<int>& keys, int size) {
        do {
            int k = KeyGenerator();
            keys.emplace(k);  // k is only inserted to keys if it's unique
        } while (keys.size() < size);
        verify(keys.size() == size);
    }

    void FBWorkload::RegisterPrecedures() {
        for (int i = 0; i < MAX_TXN_SIZE; ++i) {
            // a read
            RegP(FB_ROTXN, FB_ROTXN_P(i), {FB_REQ_VAR_ID(i)}, {}, {}, {FB_TABLE, {FB_REQ_VAR_ID(i)}}, DF_NO,
                 LPROC {
                     verify(cmd.input.size() > 0);
                     mdb::Row *r = nullptr;
                     mdb::MultiBlob mb(1);
                     mb[0] = cmd.input.at(FB_REQ_VAR_ID(i)).get_blob();
                     int key = cmd.input.at(FB_REQ_VAR_ID(i)).get_i32();
                     r = tx.Query(tx.GetTable(FB_TABLE), mb);
                     verify(!COL_COUNTS.empty());
                     verify(key < COL_COUNTS.size());
                     int n_col = COL_COUNTS.at(key);
                     std::vector<int> col_ids;
                     for (int col_id = 1; col_id <= n_col; ++col_id) { // we don't read/write the key col
                         col_ids.emplace_back(col_id);
                     }
                     std::vector<Value> results;
                     tx.ReadColumns(r, col_ids, &results, TXN_BYPASS);
                     output[FB_ROTXN_OUTPUT(i)] = results.at(0);  // we only keep the first col's result for now
                                                                      // FIXME: fix this later for real
                     *res = SUCCESS;
                }
            );
            // a write
            RegP(FB_WRITE, FB_WRITE_P(i), {FB_REQ_VAR_ID(i)}, {}, {}, {FB_TABLE, {FB_REQ_VAR_ID(i)}}, DF_NO,
                 LPROC {
                     verify(cmd.input.size() > 0);
                     mdb::Row *r = nullptr;
                     mdb::MultiBlob mb(1);
                     mb[0] = cmd.input.at(FB_REQ_VAR_ID(i)).get_blob();
                     int key = cmd.input.at(FB_REQ_VAR_ID(i)).get_i32();
                     r = tx.Query(tx.GetTable(FB_TABLE), mb);
                     verify(!COL_COUNTS.empty());
                     verify(key < COL_COUNTS.size());
                     int n_col = COL_COUNTS.at(key);
                     std::vector<int> col_ids;
                     std::vector<Value> values;
                     for (int col_id = 1; col_id <= n_col; ++col_id) { // we don't read/write the key col
                         col_ids.emplace_back(col_id);
                         values.emplace_back(get_fb_value());
                     }
                     tx.WriteColumns(r, col_ids, values, TXN_BYPASS);
                     *res = SUCCESS;
                 }
            );
        }
    }

    const Value& FBWorkload::get_fb_value() {
        verify(!FB_VALUES.empty());
        std::uniform_int_distribution<> dis(0, FB_VALUES.size() - 1);
        int index = dis(rand_gen_);
        return FB_VALUES.at(index);
    }
} // namespace janus

