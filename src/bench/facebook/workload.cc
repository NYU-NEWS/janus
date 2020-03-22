
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
        verify(single_server_ == Config::SS_DISABLED);
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
        //Value amount((i64) RandomGenerator::rand(0, 10000));
    }

    void FBWorkload::GetWriteRequest(TxRequest* req, uint32_t cid) {
        for (int i = 0; i < N_KEYS_PER_WRITE; ++i) { // fb should have 1 key per write
            int key = KeyGenerator();
            req->input_.values_->emplace(i, Value(key));
        }
    }

    void FBWorkload::GetRotxnRequest(TxRequest *req, uint32_t cid) {
        int rotxn_size = GetReadBatchSize();
        std::unordered_set<int> keys;
        GenerateReadKeys(keys, rotxn_size);
        int index = 0;
        for (const auto& key : keys) {
            req->input_.values_->emplace(index++, key);
        }
    }

    int FBWorkload::KeyGenerator() const {
        const auto& dist = Config::GetConfig()->dist_;
        if (dist == "uniform") { // uniform workloads
            std::uniform_int_distribution<> d(0, fb_para_.n_friends_ - 1);
            return d(rand_gen_);
        } else if (dist == "zipf") { // skewed workloads
            static auto alpha = Config::GetConfig()->coeffcient_;
            static ZipfDist d(alpha, fb_para_.n_friends_);
            return d(rand_gen_);  // TODO: tpca does "rotate", what is that for?
        } else {
            verify(0); // do not support other workloads for now
            return 0;
        }
    }

    int FBWorkload::GetReadBatchSize() const {
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
  RegP(TPCA_PAYMENT, TPCA_PAYMENT_1,
       {TPCA_VAR_X}, // i
       {}, // TODO o
       {conf_id_t(TPCA_CUSTOMER, {TPCA_VAR_X}, {1}, TPCA_ROW_1)}, // c
       {TPCA_CUSTOMER, {TPCA_VAR_X}}, // s
       DF_NO,
       PROC {
//        Log::debug("output: %p, output_size: %p", output, output_size);
         Value buf;
         verify(cmd.input.size() >= 1);

         mdb::Row *r = NULL;
         mdb::MultiBlob mb(1);
         mb[0] = cmd.input.at(TPCA_VAR_X).get_blob();

         r = tx.Query(tx.GetTable(TPCA_CUSTOMER), mb, TPCA_ROW_1);
//         tx.ReadColumn(r, 1, &buf, TXN_IMMEDIATE); // TODO test for janus and troad
         tx.ReadColumn(r, 1, &buf, TXN_BYPASS); // TODO test for janus and troad
         output[TPCA_VAR_OX] = buf;

         int n = tx.tid_; // making this non-commutative in order to test isolation
//         buf.set_i32(buf.get_i32() + 1/*input[1].get_i32()*/);
         buf.set_i32(n/*input[1].get_i32()*/);
         tx.WriteColumn(r, 1, buf, TXN_DEFERRED);
         *res = SUCCESS;
       }
  );

  RegP(TPCA_PAYMENT, TPCA_PAYMENT_2,
       {TPCA_VAR_Y}, // i
       {}, // o
       {conf_id_t(TPCA_CUSTOMER, {TPCA_VAR_Y}, {1}, TPCA_ROW_2)}, // c
       {TPCA_TELLER, {TPCA_VAR_Y} }, // s
       DF_REAL,
       PROC {
         Value buf;
         verify(cmd.input.size() >= 1);
         mdb::Row *r = NULL;
         mdb::MultiBlob mb(1);
         mb[0] = cmd.input.at(TPCA_VAR_Y).get_blob();

         r = tx.Query(tx.GetTable(TPCA_TELLER), mb, TPCA_ROW_2);
         tx.ReadColumn(r, 1, &buf, TXN_BYPASS);
         output[TPCA_VAR_OY] = buf;
         buf.set_i32(buf.get_i32() + 1/*input[1].get_i32()*/);
         tx.WriteColumn(r, 1, buf, TXN_DEFERRED);
         *res = SUCCESS;
       }
  );

  RegP(TPCA_PAYMENT, TPCA_PAYMENT_3,
       {TPCA_VAR_Z}, // i
       {}, // o
       {conf_id_t(TPCA_CUSTOMER, {TPCA_VAR_Z}, {1}, TPCA_ROW_3)}, // c
       {TPCA_BRANCH, {TPCA_VAR_Z}},
       DF_REAL,
       PROC {
         Value buf;
         verify(cmd.input.size() >= 1);
         i32 output_index = 0;

         mdb::Row *r = NULL;
         mdb::MultiBlob mb(1);
         mb[0] = cmd.input.at(TPCA_VAR_Z).get_blob();

         r = tx.Query(tx.GetTable(TPCA_BRANCH), mb, TPCA_ROW_3);
         tx.ReadColumn(r, 1, &buf, TXN_BYPASS);
         output[TPCA_VAR_OZ] = buf;
         buf.set_i32(buf.get_i32() + 1/*input[1].get_i32()*/);
         tx.WriteColumn(r, 1, buf, TXN_DEFERRED);
         *res = SUCCESS;
       }
  );
}

} // namespace janus

