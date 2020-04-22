
#include "deptran/__dep__.h"
#include "deptran/config.h"
#include "workload.h"
#include "zipf.h"
#include "parameters.h"

namespace janus {
    char SPANNER_TABLE[] = "f1";
    SpannerWorkload::SpannerWorkload(Config* config) : Workload(config) {
        std::map<std::string, uint64_t> table_num_rows;
        sharding_->get_number_rows(table_num_rows);
        spanner_para_.n_directories_ = table_num_rows[std::string(SPANNER_TABLE)];
        switch (single_server_) {
            case Config::SS_DISABLED:
                fix_id_ = -1;
                break;
            case Config::SS_THREAD_SINGLE:
            case Config::SS_PROCESS_SINGLE: {
                fix_id_ = RandomGenerator::rand(0, spanner_para_.n_directories_ - 1);
                unsigned int f;
                sharding_->GetPartition(SPANNER_TABLE, Value(fix_id_), f);
                break;
            }
            default:
                verify(0);
        }
        rand_gen_.seed((int)std::time(0) + (uint64_t)pthread_self());
    }

    void SpannerWorkload::GetTxRequest(TxRequest* req, uint32_t cid) {
        // decide this req is rotxn or read-write txn
        std::uniform_real_distribution<> dis(0.0, 1.0);
        double token = dis(rand_gen_); // get a token: a random double between 0 and 1
        if (token <= SPANNER_WRITE_FRACTION) { // this is a write
            req->tx_type_ = SPANNER_RW;
            GetRWRequest(req, cid);
        } else { // this is a rotxn
            req->tx_type_ = SPANNER_ROTXN;
            GetRotxnRequest(req, cid);
        }
        req->n_try_ = n_try_;
    }

    void SpannerWorkload::GetRWRequest(TxRequest* req, uint32_t cid) {
        int rw_size = GetTxnSize();
        int n_writes = GetNumWrites(rw_size);  // should be a number in 1 ~ 5, and <= rw_size, has to be at least 1 write
        std::unordered_set<int> keys;
        GenerateKeys(keys, rw_size);
        int i = 0;
        for (const auto& key : keys) {
            req->input_[SPANNER_RW_KEY(i++)] = Value(key);
        }
        req->input_[SPANNER_RW_SIZE] = Value((i32)rw_size);
        req->input_[SPANNER_RW_W_COUNT] = Value((i32)n_writes);
    }

    void SpannerWorkload::GetRotxnRequest(TxRequest *req, uint32_t cid) {
        int rotxn_size = GetTxnSize();
        std::unordered_set<int> keys;
        GenerateKeys(keys, rotxn_size);
        int index = 0;
        for (const auto& key : keys) {
            req->input_[SPANNER_ROTXN_KEY(index++)] = Value(key);
        }
        req->input_[SPANNER_ROTXN_SIZE] = Value((i32)rotxn_size);
    }

    int SpannerWorkload::KeyGenerator() {
        const auto& dist = Config::GetConfig()->dist_;
        if (dist == "uniform") { // uniform workloads
            std::uniform_int_distribution<> d(0, spanner_para_.n_directories_ - 1);
            return d(rand_gen_);
        } else if (dist == "zipf") { // skewed workloads
            static auto alpha = Config::GetConfig()->coeffcient_;
            static ZipfDist d(alpha, spanner_para_.n_directories_);
            return d(rand_gen_);  // TODO: look at tpca "rotate" for making popularity dynamic!
        } else {
            verify(0); // do not support other workloads for now
            return 0;
        }
    }

    int SpannerWorkload::GetTxnSize() {
        std::uniform_real_distribution<> dis(0.0, 1.0);
        double token = dis(rand_gen_);
        // Based on Spanner paper Table 5
        //0 = .99994050354;
        if (token < .99994050354) { return 1; }
        token -= .99994050354;

        if (token < .00005335682) { return 7; }  // the mid in [5, 9]
        token -= .00005335682;

        if (token < .00000340980) { return 3; }
        token -= .00000340980;

        if (token < .00000231986) { return 12; }
        token -= .00000231986;

        if (token < .00000033998) { return 57; }

        return 300;
    }

    void SpannerWorkload::GenerateKeys(std::unordered_set<int>& keys, int size) {
        do {
            int k = KeyGenerator();
            keys.emplace(k);  // k is only inserted to keys if it's unique
        } while (keys.size() < size);
        verify(keys.size() == size);
    }

    void SpannerWorkload::RegisterPrecedures() {
        for (int i = 0; i < MAX_TXN_SIZE; ++i) {
            // a rotxn
            set_op_type(SPANNER_ROTXN, SPANNER_ROTXN_P(i), READ_REQ);
            RegP(SPANNER_ROTXN, SPANNER_ROTXN_P(i), {SPANNER_ROTXN_KEY(i)}, {}, {}, {SPANNER_TABLE, {SPANNER_ROTXN_KEY(i)}}, DF_NO,
                 LPROC {
                     verify(cmd.input.size() > 0);
                     mdb::Row *r = nullptr;
                     mdb::MultiBlob mb(1);
                     mb[0] = cmd.input.at(SPANNER_ROTXN_KEY(i)).get_blob();
                     int key = cmd.input.at(SPANNER_ROTXN_KEY(i)).get_i32();
                     r = tx.Query(tx.GetTable(SPANNER_TABLE), mb);
                     verify(key < tx.GetTable(SPANNER_TABLE)->size());
                     Value result;
                     tx.ReadColumn(r, COL_ID, &result, TXN_BYPASS);
                     output[SPANNER_ROTXN_OUTPUT(i)] = result;
                     *res = SUCCESS;
                }
            );
            // a rw txn
            // TODO: set_op_type(SPANNER_RW, SPANNER_RW_P(i), WRITE_REQ);
            // TODO: set_write_only(FB_WRITE, FB_WRITE_P(i));
            RegP(SPANNER_RW, SPANNER_RW_P(i), {SPANNER_RW_KEY(i), SPANNER_RW_SIZE, SPANNER_RW_W_COUNT}, {}, {}, {SPANNER_TABLE, {SPANNER_RW_KEY(i)}}, DF_NO,
                 LPROC {
                     verify(cmd.input.size() > 0);
                     mdb::Row *r = nullptr;
                     mdb::MultiBlob mb(1);
                     mb[0] = cmd.input.at(SPANNER_RW_KEY(i)).get_blob();
                     // int key = cmd.input.at(SPANNER_RW_KEY(i)).get_i32();
                     r = tx.Query(tx.GetTable(SPANNER_TABLE), mb);
                     int n_write = cmd.input.at(SPANNER_RW_W_COUNT);
                     int rw_size = cmd.input.at(SPANNER_RW_SIZE);
                     if (i + n_write < rw_size) {
                         // this is read in RW
                         Value result;
                         tx.ReadColumn(r, COL_ID, &result, TXN_BYPASS);
                         output[SPANNER_RW_OUTPUT(i)] = result;
                     } else {
                         // this is a write in RW
                         Value v = get_spanner_value();
                         tx.WriteColumn(r, COL_ID, v, TXN_BYPASS);
                         // for now, no output for writes
                     }
                     *res = SUCCESS;
                 }
            );
        }
    }

    const Value& SpannerWorkload::get_spanner_value() {
        verify(!SPANNER_VALUES.empty());
        std::uniform_int_distribution<> dis(0, SPANNER_VALUES.size() - 1);
        int index = dis(rand_gen_);
        return SPANNER_VALUES.at(index);
    }
} // namespace janus

