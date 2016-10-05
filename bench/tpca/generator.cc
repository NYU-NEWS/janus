#include <deptran/txn_req_factory.h>
#include "../../deptran/config.h"
#include "generator.h"
#include "piece.h"
#include "zipf.h"

using namespace rococo;


TpcaTxnGenerator::TpcaTxnGenerator(Config* config) : TxnGenerator(config) {
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

void TpcaTxnGenerator::GetTxnReq(TxnRequest* req,
                                 uint32_t i_client,
                                 uint32_t n_client) {
  Value amount((i64) RandomGenerator::rand(0, 10000));
  req->n_try_ = n_try_;
  req->txn_type_ = TPCA_PAYMENT;
  verify(i_client < n_client);
  int k1, k2, k3;
  auto& dist = Config::GetConfig()->dist_;
  if (fix_id_ >= 0) {
    verify(dist == "fixed");
    k1 = tpca_para_.n_customer_ * i_client / n_client;
    k2 = tpca_para_.n_teller_ * i_client / n_client;
    k3 = tpca_para_.n_branch_ * i_client / n_client;
  } else if (dist == "disjoint") {
    //
    int r1 = tpca_para_.n_customer_ / n_client;
    int r2 = tpca_para_.n_teller_ / n_client;
    int r3 = tpca_para_.n_teller_ / n_client;
    boost::random::uniform_int_distribution<> d1(0, r1-1);
    boost::random::uniform_int_distribution<> d2(0, r2-1);
    boost::random::uniform_int_distribution<> d3(0, r3-1);
    k1 = r1 + d1(rand_gen_);
    k2 = r2 + d2(rand_gen_);
    k3 = r3 + d3(rand_gen_);

  } else if (dist == "uniform") {
    boost::random::uniform_int_distribution<> d1(0, tpca_para_.n_customer_-1);
    boost::random::uniform_int_distribution<> d2(0, tpca_para_.n_teller_-1);
    boost::random::uniform_int_distribution<> d3(0, tpca_para_.n_branch_-1);
    k1 = d1(rand_gen_);
    k2 = d2(rand_gen_);
    k3 = d3(rand_gen_);
//    int k1 = RandomGenerator::rand(0, tpca_para_.n_customer_ - 1);
//    int k2 = RandomGenerator::rand(0, tpca_para_.n_teller_ - 1);
//    int k3 = RandomGenerator::rand(0, tpca_para_.n_branch_ - 1);
//    Log_info("gen req, coo_id: %x \t k1: %x k2: %x, k3: %x", cid, k1, k2, k3);
  } else if (dist == "zipf") {
    static auto alpha = Config::GetConfig()->coeffcient_;
    static ZipfDist d1(alpha, tpca_para_.n_customer_);
    static ZipfDist d2(alpha, tpca_para_.n_teller_);
    static ZipfDist d3(alpha, tpca_para_.n_branch_);
    k1 = d1(rand_gen_);
    k2 = d2(rand_gen_);
    k3 = d3(rand_gen_);
  } else {
    verify(0);
  }

  req->input_ = {
      {0, Value(k1)},
      {1, Value(k2)},
      {2, Value(k3)},
      {3, amount}};
}

void TpcaTxnGenerator::GetTxnReq(TxnRequest *req, uint32_t cid) {
  Value amount((i64) RandomGenerator::rand(0, 10000));
  req->n_try_ = n_try_;
  req->txn_type_ = TPCA_PAYMENT;

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
    boost::random::uniform_int_distribution<> d1(0, tpca_para_.n_customer_-1);
    boost::random::uniform_int_distribution<> d2(0, tpca_para_.n_teller_-1);
    boost::random::uniform_int_distribution<> d3(0, tpca_para_.n_branch_-1);
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

