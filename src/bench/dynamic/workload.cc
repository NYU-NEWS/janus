
#include "deptran/__dep__.h"
#include "deptran/config.h"
#include "workload.h"
#include "zipf.h"
#include "parameters.h"

namespace janus {
    char DYNAMIC_TABLE[] = "dynamic";
    DynamicWorkload::DynamicWorkload(Config* config) : Workload(config) {
        std::map<std::string, uint64_t> table_num_rows;
        sharding_->get_number_rows(table_num_rows);
        dynamic_para_.n_rows_ = table_num_rows[std::string(DYNAMIC_TABLE)];
        switch (single_server_) {
            case Config::SS_DISABLED:
                fix_id_ = -1;
                break;
            case Config::SS_THREAD_SINGLE:
            case Config::SS_PROCESS_SINGLE: {
                fix_id_ = RandomGenerator::rand(0, dynamic_para_.n_rows_ - 1);
                unsigned int f;
                sharding_->GetPartition(DYNAMIC_TABLE, Value(fix_id_), f);
                break;
            }
            default:
                verify(0);
        }
        rand_gen_.seed((int)std::time(0) + (uint64_t)pthread_self());
    }

    void DynamicWorkload::GetTxRequest(TxRequest* req, uint32_t cid) {
        /*
        Log_info("write_fraction = %f.", Config::GetConfig()->write_fraction_);
        Log_info("txnsize = %d.", Config::GetConfig()->txn_size_);
        Log_info("winrw = %f.", Config::GetConfig()->write_in_rw_);
        Log_info("zipf = %f.", Config::GetConfig()->coeffcient_);
        Log_info("valuesize = %d.", Config::GetConfig()->value_size_);
        */
        double write_fraction = Config::GetConfig()->write_fraction_;
        // decide this req is rotxn or read-write txn
        std::uniform_real_distribution<> dis(0.0, 1.0);
        double token = dis(rand_gen_); // get a token: a random double between 0 and 1
        if (token <= write_fraction) { // this is a write
            req->tx_type_ = DYNAMIC_RW;
            GetRWRequest(req, cid);
        } else { // this is a rotxn
            req->tx_type_ = DYNAMIC_ROTXN;
            GetRotxnRequest(req, cid);
        }
        req->n_try_ = n_try_;
    }

    void DynamicWorkload::GetRWRequest(TxRequest* req, uint32_t cid) {
        int rw_size = Config::GetConfig()->txn_size_;
        double w_factor = Config::GetConfig()->write_in_rw_;
        int n_writes = 0;
        if (w_factor <= 0.00001) { // approximately 0.0
            n_writes = 1;
        } else if (w_factor >= 0.9999) { // approximately 1.0
            n_writes = rw_size;
        } else {
            n_writes = std::round(rw_size * w_factor);
        }
        verify(n_writes >= 1 && n_writes <= rw_size);
        std::unordered_set<int> keys;
        GenerateKeys(keys, rw_size);
        int i = 0;
        for (const auto& key : keys) {
            req->input_[DYNAMIC_RW_KEY(i++)] = Value(key);
        }
        req->input_[DYNAMIC_TXN_SIZE] = Value((i32)rw_size);
        req->input_[DYNAMIC_RW_W_COUNT] = Value((i32)n_writes);
        req->dynamic_rw_reads = rw_size - n_writes;
    }

    void DynamicWorkload::GetRotxnRequest(TxRequest *req, uint32_t cid) {
        int rotxn_size = Config::GetConfig()->txn_size_;
        std::unordered_set<int> keys;
        GenerateKeys(keys, rotxn_size);
        int index = 0;
        for (const auto& key : keys) {
            req->input_[DYNAMIC_ROTXN_KEY(index++)] = Value(key);
        }
        req->input_[DYNAMIC_TXN_SIZE] = Value((i32)rotxn_size);
    }

    int DynamicWorkload::KeyGenerator() {
        const auto& dist = Config::GetConfig()->dist_;
        if (dist == "uniform") { // uniform workloads
            std::uniform_int_distribution<> d(0, dynamic_para_.n_rows_ - 1);
            return d(rand_gen_);
        } else if (dist == "zipf") { // skewed workloads
            static auto alpha = Config::GetConfig()->coeffcient_;
            static ZipfDist d(alpha, dynamic_para_.n_rows_);
            return d(rand_gen_);  // TODO: look at tpca "rotate" for making popularity dynamic!
        } else {
            verify(0); // do not support other workloads for now
            return 0;
        }
    }

    void DynamicWorkload::GenerateKeys(std::unordered_set<int>& keys, int size) {
        do {
            int k = KeyGenerator();
            keys.emplace(k);  // k is only inserted to keys if it's unique
        } while (keys.size() < size);
        verify(keys.size() == size);
    }

    void DynamicWorkload::RegisterPrecedures() {
        for (int i = 0; i < Config::GetConfig()->txn_size_; ++i) {
            // a rotxn
            set_op_type(DYNAMIC_ROTXN, DYNAMIC_ROTXN_P(i), READ_REQ);
            RegP(DYNAMIC_ROTXN, DYNAMIC_ROTXN_P(i), {DYNAMIC_ROTXN_KEY(i)}, {}, {}, {DYNAMIC_TABLE, {DYNAMIC_ROTXN_KEY(i)}}, DF_NO,
                 LPROC {
                     verify(cmd.input.size() > 0);
                     mdb::Row *r = nullptr;
                     mdb::MultiBlob mb(1);
                     mb[0] = cmd.input.at(DYNAMIC_ROTXN_KEY(i)).get_blob();
                     int key = cmd.input.at(DYNAMIC_ROTXN_KEY(i)).get_i32();
                     r = tx.Query(tx.GetTable(DYNAMIC_TABLE), mb);
                     Value result;
                     tx.ReadColumn(r, COL_ID, &result, TXN_BYPASS);
                     output[DYNAMIC_ROTXN_OUTPUT(i)] = result;
                     *res = SUCCESS;
                }
            );
            // a rw txn
            set_op_type(DYNAMIC_RW, DYNAMIC_RW_P(i), WRITE_REQ);  // this could be a read, e.g., a read inside a RW
                                                                         // but is fine for now
            // TODO: set_write_only(FB_WRITE, FB_WRITE_P(i));
            RegP(DYNAMIC_RW, DYNAMIC_RW_P(i), {DYNAMIC_RW_KEY(i), DYNAMIC_TXN_SIZE, DYNAMIC_RW_W_COUNT}, {}, {}, {DYNAMIC_TABLE, {DYNAMIC_RW_KEY(i)}}, DF_NO,
                 LPROC {
                     verify(cmd.input.size() > 0);
                     mdb::Row *r = nullptr;
                     mdb::MultiBlob mb(1);
                     mb[0] = cmd.input.at(DYNAMIC_RW_KEY(i)).get_blob();
                     r = tx.Query(tx.GetTable(DYNAMIC_TABLE), mb);
                     int n_write = cmd.input.at(DYNAMIC_RW_W_COUNT).get_i32();
                     int rw_size = cmd.input.at(DYNAMIC_TXN_SIZE).get_i32();
                     if (i + n_write < rw_size) {
                         // this is read in RW
                         Value result;
                         tx.ReadColumn(r, COL_ID, &result, TXN_BYPASS);
                         output[DYNAMIC_RW_OUTPUT(i)] = result;
                     } else {
                         // this is a write in RW
                         if (DYNAMIC_VALUE.get_kind() == mdb::Value::UNKNOWN) {
                             DYNAMIC_VALUE = initialize_dynamic_value();
                         }
                         Value v = DYNAMIC_VALUE;
                         tx.WriteColumn(r, COL_ID, v, TXN_BYPASS);
                         // for now, no output for writes
                     }
                     *res = SUCCESS;
                 }
            );
        }
    }
} // namespace janus

