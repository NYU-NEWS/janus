
#include "deptran/__dep__.h"
#include "deptran/config.h"
#include "workload.h"
#include "zipf.h"

namespace janus {

char TPCA_BRANCH[] = "branch";
char TPCA_TELLER[] = "teller";
char TPCA_CUSTOMER[] = "customer";

TpcaWorkload::TpcaWorkload(Config* config) : Workload(config) {
  std::map<std::string, uint64_t> table_num_rows;
  sharding_->get_number_rows(table_num_rows);
  tpca_para_.n_branch_ = table_num_rows[std::string(TPCA_BRANCH)];
  tpca_para_.n_teller_ = table_num_rows[std::string(TPCA_TELLER)];
  tpca_para_.n_customer_ = table_num_rows[std::string(TPCA_CUSTOMER)];
  switch (single_server_) {
    case Config::SS_DISABLED:
      fix_id_ = -1;
      break;
    case Config::SS_THREAD_SINGLE:
    case Config::SS_PROCESS_SINGLE: {
      verify(tpca_para_.n_branch_ == tpca_para_.n_teller_
                 && tpca_para_.n_branch_ == tpca_para_.n_customer_);
      fix_id_ = RandomGenerator::rand(0, tpca_para_.n_branch_ - 1);
      unsigned int b, t, c;
      sharding_->GetPartition(TPCA_BRANCH, Value(fix_id_), b);
      sharding_->GetPartition(TPCA_TELLER, Value(fix_id_), t);
      sharding_->GetPartition(TPCA_CUSTOMER, Value(fix_id_), c);
      verify(b == c && c == t);
      break;
    }
    default:
      verify(0);
  }
  rand_gen_.seed((int)std::time(0) + (uint64_t)pthread_self());
}

//void TpcaWorkload::GetTxRequest(TxnRequest* req,
//                             uint32_t i_client,
//                             uint32_t n_client) {
//  Value amount((i64) RandomGenerator::rand(0, 10000));
//  req->n_try_ = n_try_;
//  req->tx_type_ = TPCA_PAYMENT;
//  verify(i_client < n_client);
//  int k1, k2, k3;
//  auto& dist = Config::GetConfig()->dist_;
//  if (fix_id_ >= 0) {
//    verify(dist == "fixed");
//    k1 = tpca_para_.n_customer_ * i_client / n_client;
//    k2 = tpca_para_.n_teller_ * i_client / n_client;
//    k3 = tpca_para_.n_branch_ * i_client / n_client;
//  } else if (dist == "disjoint") {
//    //
//    int r1 = tpca_para_.n_customer_ / n_client;
//    int r2 = tpca_para_.n_teller_ / n_client;
//    int r3 = tpca_para_.n_teller_ / n_client;
//    boost::random::uniform_int_distribution<> d1(0, r1-1);
//    boost::random::uniform_int_distribution<> d2(0, r2-1);
//    boost::random::uniform_int_distribution<> d3(0, r3-1);
//    k1 = r1 + d1(rand_gen_);
//    k2 = r2 + d2(rand_gen_);
//    k3 = r3 + d3(rand_gen_);
//
//  } else if (dist == "uniform") {
//    boost::random::uniform_int_distribution<> d1(0, tpca_para_.n_customer_-1);
//    boost::random::uniform_int_distribution<> d2(0, tpca_para_.n_teller_-1);
//    boost::random::uniform_int_distribution<> d3(0, tpca_para_.n_branch_-1);
//    k1 = d1(rand_gen_);
//    k2 = d2(rand_gen_);
//    k3 = d3(rand_gen_);
////    int k1 = RandomGenerator::rand(0, tpca_para_.n_customer_ - 1);
////    int k2 = RandomGenerator::rand(0, tpca_para_.n_teller_ - 1);
////    int k3 = RandomGenerator::rand(0, tpca_para_.n_branch_ - 1);
////    Log_info("gen req, coo_id: %x \t k1: %x k2: %x, k3: %x", cid, k1, k2, k3);
//  } else if (dist == "zipf") {
//    static auto alpha = Config::GetConfig()->coeffcient_;
//    static ZipfDist d1(alpha, tpca_para_.n_customer_);
//    static ZipfDist d2(alpha, tpca_para_.n_teller_);
//    static ZipfDist d3(alpha, tpca_para_.n_branch_);
//    k1 = d1(rand_gen_);
//    k2 = d2(rand_gen_);
//    k3 = d3(rand_gen_);
//  } else {
//    verify(0);
//  }

//  req->input_ = {
//      {0, Value(k1)},
//      {1, Value(k2)},
//      {2, Value(k3)},
//      {3, amount}};
//}

void TpcaWorkload::GetTxRequest(TxRequest* req, uint32_t cid) {
  Value amount((i64) RandomGenerator::rand(0, 10000));
  req->n_try_ = n_try_;
  req->tx_type_ = TPCA_PAYMENT;

  auto& dist = Config::GetConfig()->dist_;
  auto& rotate = Config::GetConfig()->rotate_;
  if (fix_id_ >= 0) {
    verify(dist == "fixed");
    // TODO, divide the key range, choose a different one each time.
    if (key_ids_.count(cid) == 0) {
      int32_t& key = key_ids_[cid];
      key = fix_id_;
      key += (cid & 0xFFFFFFFF);
      key = key >= 0 ? key : -key;
      key %= tpca_para_.n_branch_;
      Log_debug("choosing key, coo_id: %x \t key: %llx", cid, key);
    }
    int32_t& key = key_ids_[cid];
    req->input_ = {
        {TPCA_VAR_X, Value((i32) key)},
        {TPCA_VAR_Y, Value((i32) key)},
        {TPCA_VAR_Z, Value((i32) key)},
        {TPCA_VAR_AMOUNT, amount}
    };
  } else if (dist == "disjoint") {
    //

  } else if (dist == "uniform") {
    std::uniform_int_distribution<> d1(0, tpca_para_.n_customer_-1);
    std::uniform_int_distribution<> d2(0, tpca_para_.n_teller_-1);
    std::uniform_int_distribution<> d3(0, tpca_para_.n_branch_-1);
    int k1 = d1(rand_gen_);
    int k2 = d2(rand_gen_);
    int k3 = d3(rand_gen_);
    req->input_ = {
        {0, Value(k1)},
        {1, Value(k2)},
        {2, Value(k3)},
        {3, amount}};
  } else if (dist == "zipf") {
    static auto alpha = Config::GetConfig()->coeffcient_;
    static ZipfDist d1(alpha, tpca_para_.n_customer_);
    static ZipfDist d2(alpha, tpca_para_.n_teller_);
    static ZipfDist d3(alpha, tpca_para_.n_branch_);
    int k1 = d1(rand_gen_);
    int k2 = d2(rand_gen_);
    int k3 = d3(rand_gen_);
    if (rotate > 0) {
      int n1 = tpca_para_.n_customer_ / rotate;
      int n2 = tpca_para_.n_teller_ /   rotate;
      int n3 = tpca_para_.n_branch_ /   rotate;
      k2 = (k2 + n2) % tpca_para_.n_teller_;
      k3 = (k3 + n3*2) % tpca_para_.n_branch_;
    }
    req->input_ = {
        {0, Value(k1)},
        {1, Value(k2)},
        {2, Value(k3)},
        {3, amount}};
  } else {
    verify(0);
  }
}


void TpcaWorkload::RegisterPrecedures() {
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
         tx.ReadColumn(r, 1, &buf, RANK_I); // TODO test for janus and troad
         output[TPCA_VAR_OX] = buf;

         int n = tx.tid_; // making this non-commutative in order to test isolation
//         buf.set_i32(buf.get_i32() + 1/*input[1].get_i32()*/);
         buf.set_i32(n/*input[1].get_i32()*/);
         tx.WriteColumn(r, 1, buf, RANK_I);
         *res = SUCCESS;
       }
  );

  RegP(TPCA_PAYMENT, TPCA_PAYMENT_2,
       {TPCA_VAR_Y}, // i
       {}, // o
       {conf_id_t(TPCA_TELLER, {TPCA_VAR_Y}, {1}, TPCA_ROW_2)}, // c
       {TPCA_TELLER, {TPCA_VAR_Y} }, // s
       DF_REAL,
       PROC {
         auto buf = std::make_unique<Value>();
         verify(cmd.input.size() >= 1);
         mdb::Row *r = NULL;
         mdb::MultiBlob mb(1);
         mb[0] = cmd.input.at(TPCA_VAR_Y).get_blob();

         r = tx.Query(tx.GetTable(TPCA_TELLER), mb, TPCA_ROW_2);
         tx.ReadColumn(r, 1, buf.get(), TXN_BYPASS);
         output[TPCA_VAR_OY] = *buf;
         buf->set_i32(buf->get_i32() + 1/*input[1].get_i32()*/);
         tx.WriteColumn(r, 1, *buf, TXN_DEFERRED);
         *res = SUCCESS;
       }
  );

  RegP(TPCA_PAYMENT, TPCA_PAYMENT_3,
       {TPCA_VAR_Z}, // i
       {}, // o
       {conf_id_t(TPCA_BRANCH, {TPCA_VAR_Z}, {1}, TPCA_ROW_3)}, // c
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

