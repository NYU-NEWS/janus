#include "deptran/config.h"
#include "generator.h"
#include "piece.h"

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
}

void TpcaTxnGenerator::GetTxnReq(TxnRequest *req, uint32_t cid) {
  Value amount((i64) RandomGenerator::rand(0, 10000));
  req->n_try_ = n_try_;
  req->txn_type_ = TPCA_PAYMENT;

  if (fix_id_ >= 0) {
    if (key_ids_.count(cid) == 0) {
      int32_t& key = key_ids_[cid];
      key = fix_id_;
      key += (cid & 0xFFFFFFFF);
      key = key >= 0 ? key : -key;
      key %= tpca_para_.n_branch_;
      Log_info("choosing key, coo_id: %x \t key: %llx", cid, key);
    }
    int32_t& key = key_ids_[cid];
    req->input_ = {
        {0, Value((i32) key)},
        {1, Value((i32) key)},
        {2, Value((i32) key)},
        {3, amount}
    };
  } else {
    req->input_ = {
        {0, Value((i32) RandomGenerator::rand(0, tpca_para_.n_customer_ - 1))},
        {1, Value((i32) RandomGenerator::rand(0, tpca_para_.n_teller_ - 1))},
        {2, Value((i32) RandomGenerator::rand(0, tpca_para_.n_branch_ - 1))},
        {3, amount}
    };
  }
}