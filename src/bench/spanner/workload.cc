
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
        double write_fraction = Config::GetConfig()->write_fraction_;
        // if (token <= SPANNER_WRITE_FRACTION) { // this is a write
        if (token <= write_fraction) {
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
        int n_writes = GetNumWrites(rw_size);  // should be a number in 1 ~ 3, and <= rw_size, has to be at least 1 write
        std::unordered_set<int> keys;
        GenerateKeys(keys, rw_size);
        int i = 0;
        for (const auto& key : keys) {
            req->input_[SPANNER_RW_KEY(i++)] = Value(key);
        }
        req->input_[SPANNER_TXN_SIZE] = Value((i32)rw_size);
        req->input_[SPANNER_RW_W_COUNT] = Value((i32)n_writes);
        req->spanner_rw_reads = rw_size - n_writes;
    }

    void SpannerWorkload::GetRotxnRequest(TxRequest *req, uint32_t cid) {
        int rotxn_size = GetTxnSize();
        std::unordered_set<int> keys;
        GenerateKeys(keys, rotxn_size);
        int index = 0;
        for (const auto& key : keys) {
            req->input_[SPANNER_ROTXN_KEY(index++)] = Value(key);
        }
        req->input_[SPANNER_TXN_SIZE] = Value((i32)rotxn_size);
    }

    int SpannerWorkload::KeyGenerator() {
        const auto& dist = Config::GetConfig()->dist_;
        if (dist == "uniform") { // uniform workloads
            std::uniform_int_distribution<> d(0, N_CORE_KEYS - 1);
            return d(rand_gen_);
        } else if (dist == "zipf") { // skewed workloads
            static auto alpha = Config::GetConfig()->coeffcient_;
            static ZipfDist d(alpha, N_CORE_KEYS);
            // return d(rand_gen_);  // TODO: look at tpca "rotate" for making popularity dynamic!
            int zipf_key = d(rand_gen_);  // TODO: tpca does "rotate", what is that for?
            if (zipf_key >= 0 && zipf_key < N_CORE_KEYS && !SHUFFLED_KEYS.empty()) {
                // verify(!SHUFFLED_KEYS.empty());
                return SHUFFLED_KEYS[zipf_key];
            } else {
                // verify(0);
                return zipf_key;
            }
        } else {
            verify(0); // do not support other workloads for now
            return 0;
        }
    }

    int SpannerWorkload::GetTxnSize() {
        std::uniform_real_distribution<> dis(1.0, 10.0);
        return std::round(dis(rand_gen_));
        /*
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
        */
    }

    int SpannerWorkload::GetNumWrites(int rw_size) {
        return 1;   // based on F1 paper, r-w txn always has exactly 1 write at the end
        /*
        // get at least 1 write, at most 3 writes
        if (rw_size == 1) {
            return 1;
        }
        std::uniform_real_distribution<> dis(0.0, 1.0);
        double token = dis(rand_gen_);
        if (token < .5) { return 1; }
        token -= .5;

        if (token < .25 || rw_size == 2) {
            return 2;
        } else {
            return 3;
        }
        */
    }

    void SpannerWorkload::GenerateKeys(std::unordered_set<int>& keys, int size) {
        do {
            int core_key = KeyGenerator();
            int real_key = CoreKeyMapping(core_key, spanner_para_.n_directories_);
            keys.emplace(real_key);  // k is only inserted to keys if it's unique
        } while (keys.size() < size);
        verify(keys.size() == size);
    }

    int SpannerWorkload::CoreKeyMapping(int core_key, int n_total_keys) const {
        verify(core_key < n_total_keys && core_key < N_CORE_KEYS);
        int amplifier = n_total_keys / N_CORE_KEYS;
        std::uniform_int_distribution<> dis(0, amplifier - 1);
        int multiplier = dis(RAND_SPANNER_KEY_MAPPING);
        int real_key = core_key + multiplier * N_CORE_KEYS;
        verify(real_key < n_total_keys);
        return real_key;
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
                     r->key = key;
                     Value result;
                     tx.ReadColumn(r, COL_ID, &result, TXN_BYPASS);
                     output[SPANNER_ROTXN_OUTPUT(i)] = result;
                     *res = SUCCESS;
                }
            );
            // a rw txn
            set_op_type(SPANNER_RW, SPANNER_RW_P(i), WRITE_REQ);  // this could be a read, e.g., a read inside a RW
                                                                         // but is fine for now
            // TODO: set_write_only(FB_WRITE, FB_WRITE_P(i));
            RegP(SPANNER_RW, SPANNER_RW_P(i), {SPANNER_RW_KEY(i), SPANNER_TXN_SIZE, SPANNER_RW_W_COUNT}, {}, {}, {SPANNER_TABLE, {SPANNER_RW_KEY(i)}}, DF_NO,
                 LPROC {
                     verify(cmd.input.size() > 0);
                     mdb::Row *r = nullptr;
                     mdb::MultiBlob mb(1);
                     mb[0] = cmd.input.at(SPANNER_RW_KEY(i)).get_blob();
                     int key = cmd.input.at(SPANNER_RW_KEY(i)).get_i32();
                     r = tx.Query(tx.GetTable(SPANNER_TABLE), mb);
                     r->key = key;
                     int n_write = cmd.input.at(SPANNER_RW_W_COUNT).get_i32();
                     int rw_size = cmd.input.at(SPANNER_TXN_SIZE).get_i32();
                     if (i + n_write < rw_size) {
                         // this is read in RW
                         Value result;
                         tx.ReadColumn(r, COL_ID, &result, TXN_BYPASS);
                         output[SPANNER_RW_OUTPUT(i)] = result;
                     } else {
                         // this is a write in RW, we do a read first
                         // Value result;
                         //tx.ReadColumn(r, COL_ID, &result, TXN_BYPASS);
                         Value v = get_spanner_value();
                         tx.WriteColumn(r, COL_ID, v, TXN_BYPASS);
                         // tx.WriteColumn(r, COL_ID, result, TXN_DEFERRED);
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

