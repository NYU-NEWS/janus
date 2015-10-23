#include "config.h"
#include "constants.h"
#include "sharding.h"
#include "txn_req_factory.h"

// for tpca benchmark
#include "bench/tpca/piece.h"
#include "bench/tpca/chopper.h"

// tpcc benchmark
#include "bench/tpcc/piece.h"
#include "bench/tpcc/chopper.h"

// tpcc dist partition benchmark
#include "bench/tpcc_dist/piece.h"
#include "bench/tpcc_dist/chopper.h"

// tpcc real dist partition benchmark
#include "bench/tpcc_real_dist/piece.h"
#include "bench/tpcc_real_dist/chopper.h"

// rw benchmark
#include "bench/rw_benchmark/piece.h"
#include "bench/rw_benchmark/chopper.h"

// micro bench
#include "bench/micro/piece.h"
#include "bench/micro/chopper.h"

#include "txn-chopper-factory.h"

namespace rococo {

//TxnRequestFactory *TxnRequestFactory::txn_req_factory_s = NULL;

TxnRequestFactory::TxnRequestFactory(Sharding* sd)
    : txn_weight_(Config::GetConfig()->get_txn_weight()),
      sharding_(sd){
  benchmark_ = Config::GetConfig()->get_benchmark();
  n_try_ = Config::GetConfig()->get_max_retry();
  single_server_ = Config::GetConfig()->get_single_server();

  std::map<std::string, uint64_t> table_num_rows;
  sharding_->get_number_rows(table_num_rows);

  switch (benchmark_) {
    case MICRO_BENCH:
      micro_bench_para_.n_table_a_ = table_num_rows[std::string(MICRO_BENCH_TABLE_A)];
      micro_bench_para_.n_table_b_ = table_num_rows[std::string(MICRO_BENCH_TABLE_B)];
      micro_bench_para_.n_table_c_ = table_num_rows[std::string(MICRO_BENCH_TABLE_C)];
      micro_bench_para_.n_table_d_ = table_num_rows[std::string(MICRO_BENCH_TABLE_D)];
      break;
    case TPCA:
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
          sharding_->get_site_id_from_tb(TPCA_BRANCH, Value(fix_id_), b);
          sharding_->get_site_id_from_tb(TPCA_TELLER, Value(fix_id_), t);
          sharding_->get_site_id_from_tb(TPCA_CUSTOMER, Value(fix_id_), c);
          verify(b == c && c == t);
          break;
        }
        default:
          verify(0);
      }
      break;
    case TPCC:
    case TPCC_DIST_PART: {
      std::vector<unsigned int> sites;
      sharding_->get_site_id_from_tb(TPCC_TB_WAREHOUSE, sites);
      uint64_t tb_w_rows = table_num_rows[std::string(TPCC_TB_WAREHOUSE)];
      tpcc_para_.n_w_id_ = (int) tb_w_rows * sites.size();
      tpcc_para_.const_home_w_id_ = RandomGenerator::rand(0, tpcc_para_.n_w_id_ - 1);
      uint64_t tb_d_rows = table_num_rows[std::string(TPCC_TB_DISTRICT)];
      tpcc_para_.n_d_id_ = (int) tb_d_rows / tb_w_rows;
      uint64_t tb_c_rows = table_num_rows[std::string(TPCC_TB_CUSTOMER)];
      tpcc_para_.n_c_id_ = (int) tb_c_rows / tb_d_rows;
      tpcc_para_.n_i_id_ = (int) table_num_rows[std::string(TPCC_TB_ITEM)];
      tpcc_para_.delivery_d_id_ = RandomGenerator::rand(0, tpcc_para_.n_d_id_ - 1);
      if (single_server_ != Config::SS_DISABLED) {
        verify(0);
      }
      else
        fix_id_ = -1;
      break;
    }
    case TPCC_REAL_DIST_PART: {
      std::vector<unsigned int> sites;
      sharding_->get_site_id_from_tb(TPCC_TB_WAREHOUSE, sites);
      uint64_t tb_w_rows = table_num_rows[std::string(TPCC_TB_WAREHOUSE)];
      tpcc_para_.n_w_id_ = (int) tb_w_rows;
      tpcc_para_.const_home_w_id_ = RandomGenerator::rand(0, tpcc_para_.n_w_id_ - 1);
      uint64_t tb_d_rows = table_num_rows[std::string(TPCC_TB_DISTRICT)];
      tpcc_para_.n_d_id_ = (int) tb_d_rows * sites.size() / tb_w_rows;
      uint64_t tb_c_rows = table_num_rows[std::string(TPCC_TB_CUSTOMER)];
      tpcc_para_.n_c_id_ = (int) tb_c_rows / tb_d_rows;
      tpcc_para_.n_i_id_ = (int) table_num_rows[std::string(TPCC_TB_ITEM)];
      tpcc_para_.delivery_d_id_ = RandomGenerator::rand(0, tpcc_para_.n_d_id_ - 1);
      switch (single_server_) {
        case Config::SS_DISABLED:
          fix_id_ = -1;
          break;
        case Config::SS_THREAD_SINGLE:
        case Config::SS_PROCESS_SINGLE: {
          fix_id_ = Config::GetConfig()->get_client_id() % tpcc_para_.n_d_id_;
          tpcc_para_.const_home_w_id_ =
              Config::GetConfig()->get_client_id() / tpcc_para_.n_d_id_;
          break;
        }
        default:
          verify(0);
      }
      break;
    }
    case RW_BENCHMARK:
      rw_benchmark_para_.n_table_ = table_num_rows[std::string(RW_BENCHMARK_TABLE)];
      break;
    default:
      Log_fatal("benchmark not implemented");
      verify(0);
  }
}

void TxnRequestFactory::get_rw_benchmark_w_txn_req(TxnRequest *req, uint32_t cid) const {
  req->txn_type_ = RW_BENCHMARK_W_TXN;
  req->input_.assign({
                         Value((i32) RandomGenerator::rand(0, rw_benchmark_para_.n_table_ - 1)),
                         Value((i32) RandomGenerator::rand(0, 10000))
                     });
}

void TxnRequestFactory::get_rw_benchmark_r_txn_req(TxnRequest *req, uint32_t cid) const {
  req->txn_type_ = RW_BENCHMARK_R_TXN;
  req->input_.assign({
                         Value((i32) RandomGenerator::rand(0, rw_benchmark_para_.n_table_ - 1))
                     });
}

void TxnRequestFactory::get_rw_benchmark_txn_req(TxnRequest *req, uint32_t cid) const {
  req->n_try_ = n_try_;
  if (txn_weight_.size() != 2)
    get_rw_benchmark_r_txn_req(req, cid);
  else
    switch (RandomGenerator::weighted_select(txn_weight_)) {
      case 0: // read
        get_rw_benchmark_r_txn_req(req, cid);
        break;
      case 1: // write
        get_rw_benchmark_w_txn_req(req, cid);
        break;
      default:
        verify(0);
    }
}

void TxnRequestFactory::get_tpca_txn_req(TxnRequest *req, uint32_t cid) const {
  Value amount((i64) RandomGenerator::rand(0, 10000));
  req->n_try_ = n_try_;
  req->txn_type_ = TPCA_PAYMENT;
  if (fix_id_ >= 0) {
    int fix_tid = fix_id_;
    if (single_server_ == Config::SS_THREAD_SINGLE) {
      unsigned long s = (unsigned long) pthread_self();
      unsigned int s2 = (unsigned int) s;
      int r = rand_r(&s2);
      fix_tid += r;
      fix_tid = fix_tid >= 0 ? fix_tid : -fix_tid;
      fix_tid %= tpca_para_.n_branch_;
    }
    req->input_.assign({Value((i32) fix_tid), Value((i32) fix_tid), Value((i32) fix_tid), amount});
  }
  else
    req->input_.assign({
                           Value((i32) RandomGenerator::rand(0, tpca_para_.n_customer_ - 1)),
                           Value((i32) RandomGenerator::rand(0, tpca_para_.n_teller_ - 1)),
                           Value((i32) RandomGenerator::rand(0, tpca_para_.n_branch_ - 1)),
                           amount});
}

void TxnRequestFactory::get_tpcc_new_order_txn_req(TxnRequest *req, uint32_t cid) const {
  req->txn_type_ = TPCC_NEW_ORDER;
  //int home_w_id = RandomGenerator::rand(0, tpcc_para_.n_w_id_ - 1);
  int home_w_id = cid % tpcc_para_.n_w_id_;
  Value w_id((i32) home_w_id);
  //Value d_id((i32)RandomGenerator::rand(0, tpcc_para_.n_d_id_ - 1));
  Value d_id((i32) (cid / tpcc_para_.n_w_id_) % tpcc_para_.n_d_id_);
  Value c_id((i32) RandomGenerator::nu_rand(1022, 0, tpcc_para_.n_c_id_ - 1));
  int ol_cnt = RandomGenerator::rand(5, 15);
  //int ol_cnt;

  //ol_cnt= RandomGenerator::rand(10, 10);
  //if (ol_cnt != 10 ) {
  //    Log::error("lalalalal, random wrong! %d", ol_cnt);
  //    verify(ol_cnt == 10);
  //}
  //ol_cnt= RandomGenerator::rand(2, 2);

  rrr::i32 i_id_buf[ol_cnt];
  req->input_.resize(4 + 3 * ol_cnt);
  //if (fix_id_ >= 0) {
  //    req->input_[0] = tpcc_para_.const_home_w_id_;
  //    req->input_[1] = fix_id_;
  //}
  //else {
  req->input_[0] = w_id;
  req->input_[1] = d_id;
  //}
  req->input_[2] = c_id;
  req->input_[3] = Value((i32) ol_cnt);
  int i = 0;
  for (; i < ol_cnt; i++) {
    //req->input_[4 + 3 * i] = Value((i32)RandomGenerator::nu_rand(8191, 0, tpcc_para_.n_i_id_ - 1)); XXX nurand is the standard
    rrr::i32 tmp_i_id = (i32) RandomGenerator::rand(0, tpcc_para_.n_i_id_ - 1 - i);

    int pre_n_less = 0, n_less = 0;
    while (true) {
      n_less = 0;
      for (int j = 0; j < i; j++)
        if (i_id_buf[j] <= tmp_i_id)
          n_less++;
      if (n_less == pre_n_less)
        break;
      tmp_i_id += (n_less - pre_n_less);
      pre_n_less = n_less;
    }

    i_id_buf[i] = tmp_i_id;
    req->input_[4 + 3 * i] = Value(tmp_i_id);

    if (tpcc_para_.n_w_id_ > 1 && // warehouse more than one, can do remote
        RandomGenerator::percentage_true(1)) { //XXX 1% REMOTE_RATIO
      int remote_w_id = RandomGenerator::rand(0, tpcc_para_.n_w_id_ - 2);
      remote_w_id = remote_w_id >= home_w_id ? remote_w_id + 1 : remote_w_id;
      req->input_[5 + 3 * i] = Value((i32) remote_w_id);
    }
    else
      req->input_[5 + 3 * i] = req->input_[0];
    req->input_[6 + 3 * i] = Value((i32) RandomGenerator::rand(0, 10));
  }

  //for (i = 0; i < ol_cnt; i++) {
  //    verify(i_id_buf[i] < tpcc_para_.n_i_id_ && i_id_buf[i] >= 0);
  //    for (int j = i + 1; j < ol_cnt; j++)
  //        verify(i_id_buf[i] != i_id_buf[j]);
  //}

}

void TxnRequestFactory::get_tpcc_payment_txn_req(TxnRequest *req, uint32_t cid) const {
  req->txn_type_ = TPCC_PAYMENT;
  //int home_w_id = RandomGenerator::rand(0, tpcc_para_.n_w_id_ - 1);
  int home_w_id = cid % tpcc_para_.n_w_id_;
  Value c_w_id, c_d_id;
  Value w_id((i32) home_w_id);
  //Value d_id((i32)RandomGenerator::rand(0, tpcc_para_.n_d_id_ - 1));
  Value d_id((i32) (cid / tpcc_para_.n_w_id_) % tpcc_para_.n_d_id_);
  Value c_id_or_last;
  if (RandomGenerator::percentage_true(60)) //XXX query by last name 60%
    c_id_or_last = Value(RandomGenerator::int2str_n(RandomGenerator::nu_rand(255, 0, 999), 3));
  else
    c_id_or_last = Value((i32) RandomGenerator::nu_rand(1022, 0, tpcc_para_.n_c_id_ - 1));
  if (tpcc_para_.n_w_id_ > 1 && // warehouse more than one, can do remote
      RandomGenerator::percentage_true(15)) { //XXX 15% pay through remote warehouse, 85 home REMOTE_RATIO
    int c_w_id_int = RandomGenerator::rand(0, tpcc_para_.n_w_id_ - 2);
    c_w_id_int = c_w_id_int >= home_w_id ? c_w_id_int + 1 : c_w_id_int;
    c_w_id = Value((i32) c_w_id_int);
    c_d_id = Value((i32) RandomGenerator::rand(0, tpcc_para_.n_d_id_ - 1));
  }
  else {
    c_w_id = w_id;
    c_d_id = d_id;
  }
  Value h_amount(RandomGenerator::rand_double(1.00, 5000.00));
  req->input_.resize(7);
  req->input_[0] = w_id;
  req->input_[1] = d_id;
  req->input_[2] = c_id_or_last;
  req->input_[3] = c_w_id;
  req->input_[4] = c_d_id;
  req->input_[5] = h_amount;
  req->input_[6] = Value((i32) RandomGenerator::rand()); // h_key
}

void TxnRequestFactory::get_tpcc_stock_level_txn_req(TxnRequest *req, uint32_t cid) const {
  req->txn_type_ = TPCC_STOCK_LEVEL;
  req->input_.resize(3);
  //req->input_[0] = Value((i32)RandomGenerator::rand(0, tpcc_para_.n_w_id_ - 1));
  //req->input_[1] = Value((i32)RandomGenerator::rand(0, tpcc_para_.n_d_id_ - 1));
  req->input_[0] = Value((i32) (cid % tpcc_para_.n_w_id_));
  req->input_[1] = Value((i32) (cid / tpcc_para_.n_w_id_) % tpcc_para_.n_d_id_);
  req->input_[2] = Value((i32) RandomGenerator::rand(10, 20));
}

void TxnRequestFactory::get_tpcc_delivery_txn_req(TxnRequest *req, uint32_t cid) const {
  req->txn_type_ = TPCC_DELIVERY;
  req->input_.resize(3);
  //req->input_[0] = Value((i32)RandomGenerator::rand(0, tpcc_para_.n_w_id_ - 1));
  req->input_[0] = Value((i32) (cid % tpcc_para_.n_w_id_));
  req->input_[1] = Value((i32) RandomGenerator::rand(1, 10));
  //req->input_[2] = Value((i32)tpcc_para_.delivery_d_id_++);
  //if (tpcc_para_.delivery_d_id_ >= tpcc_para_.n_d_id_)
  //    tpcc_para_.delivery_d_id_ = 0;
  req->input_[2] = Value((i32) (cid / tpcc_para_.n_w_id_) % tpcc_para_.n_d_id_);
}

void TxnRequestFactory::get_tpcc_order_status_txn_req(TxnRequest *req, uint32_t cid) const {
  req->txn_type_ = TPCC_ORDER_STATUS;
  Value c_id_or_last;
  if (RandomGenerator::percentage_true(60)) //XXX 60% by c_last
    c_id_or_last = Value(RandomGenerator::int2str_n(RandomGenerator::nu_rand(255, 0, 999), 3));
  else
    c_id_or_last = Value((i32) RandomGenerator::nu_rand(1022, 0, tpcc_para_.n_c_id_ - 1));
  req->input_.resize(3);
  //req->input_[0] = Value((i32)RandomGenerator::rand(0, tpcc_para_.n_w_id_ - 1));
  //req->input_[1] = Value((i32)RandomGenerator::rand(0, tpcc_para_.n_d_id_ - 1));
  req->input_[0] = Value((i32) (cid % tpcc_para_.n_w_id_));
  req->input_[1] = Value((i32) ((cid / tpcc_para_.n_w_id_) % tpcc_para_.n_d_id_));
  req->input_[2] = c_id_or_last;
}

void TxnRequestFactory::get_micro_bench_read_req(TxnRequest *req, uint32_t cid) const {
  req->txn_type_ = MICRO_BENCH_R;
  req->input_.resize(4);
  req->input_[0] = Value((i32) RandomGenerator::rand(0, micro_bench_para_.n_table_a_ - 1));
  req->input_[1] = Value((i32) RandomGenerator::rand(0, micro_bench_para_.n_table_b_ - 1));
  req->input_[2] = Value((i32) RandomGenerator::rand(0, micro_bench_para_.n_table_c_ - 1));
  req->input_[3] = Value((i32) RandomGenerator::rand(0, micro_bench_para_.n_table_d_ - 1));
}

void TxnRequestFactory::get_micro_bench_write_req(TxnRequest *req, uint32_t cid) const {
  req->txn_type_ = MICRO_BENCH_W;
  req->input_.resize(8);
  req->input_[0] = Value((i32) RandomGenerator::rand(0, micro_bench_para_.n_table_a_ - 1));
  req->input_[1] = Value((i32) RandomGenerator::rand(0, micro_bench_para_.n_table_b_ - 1));
  req->input_[2] = Value((i32) RandomGenerator::rand(0, micro_bench_para_.n_table_c_ - 1));
  req->input_[3] = Value((i32) RandomGenerator::rand(0, micro_bench_para_.n_table_d_ - 1));
  req->input_[4] = Value((i64) RandomGenerator::rand(0, 1000));
  req->input_[5] = Value((i64) RandomGenerator::rand(0, 1000));
  req->input_[6] = Value((i64) RandomGenerator::rand(0, 1000));
  req->input_[7] = Value((i64) RandomGenerator::rand(0, 1000));
}

void TxnRequestFactory::get_micro_bench_txn_req(TxnRequest *req, uint32_t cid) const {
  req->n_try_ = n_try_;
  if (txn_weight_.size() != 2)
    get_micro_bench_read_req(req, cid);
  else
    switch (RandomGenerator::weighted_select(txn_weight_)) {
      case 0: // read
        get_micro_bench_read_req(req, cid);
        break;
      case 1: // write
        get_micro_bench_write_req(req, cid);
        break;
    }

}

void TxnRequestFactory::get_tpcc_txn_req(TxnRequest *req, uint32_t cid) const {
  req->n_try_ = n_try_;
  if (txn_weight_.size() != 5) {
    verify(0);
    get_tpcc_new_order_txn_req(req, cid);
  } else {
    switch (RandomGenerator::weighted_select(txn_weight_)) {
      case 0:
        get_tpcc_new_order_txn_req(req, cid);
        break;
      case 1:
        get_tpcc_payment_txn_req(req, cid);
        break;
      case 2:
        get_tpcc_order_status_txn_req(req, cid);
        break;
      case 3:
        get_tpcc_delivery_txn_req(req, cid);
        break;
      case 4:
        get_tpcc_stock_level_txn_req(req, cid);
        break;
      default:
        verify(0);
    }
  }
}

void TxnRequestFactory::get_txn_req(TxnRequest *req, uint32_t cid) const {
  switch (benchmark_) {
    case TPCA:
      get_tpca_txn_req(req, cid);
      break;
    case TPCC:
    case TPCC_DIST_PART:
    case TPCC_REAL_DIST_PART:
      get_tpcc_txn_req(req, cid);
      break;
    case RW_BENCHMARK:
      get_rw_benchmark_txn_req(req, cid);
      break;
    case MICRO_BENCH:
      get_micro_bench_txn_req(req, cid);
      break;
    default:
      Log_fatal("benchmark not implemented");
      verify(0);
  }
}

void TxnRequestFactory::get_txn_types(std::map<int32_t, std::string> &txn_types) {
  txn_types.clear();
  switch (benchmark_) {
    case TPCA:
      txn_types[TPCA_PAYMENT] = std::string(TPCA_PAYMENT_NAME);
      break;
    case TPCC:
    case TPCC_DIST_PART:
    case TPCC_REAL_DIST_PART:
      txn_types[TPCC_NEW_ORDER] = std::string(TPCC_NEW_ORDER_NAME);
      txn_types[TPCC_PAYMENT] = std::string(TPCC_PAYMENT_NAME);
      txn_types[TPCC_STOCK_LEVEL] = std::string(TPCC_STOCK_LEVEL_NAME);
      txn_types[TPCC_DELIVERY] = std::string(TPCC_DELIVERY_NAME);
      txn_types[TPCC_ORDER_STATUS] = std::string(TPCC_ORDER_STATUS_NAME);
      break;
    case RW_BENCHMARK:
      txn_types[RW_BENCHMARK_W_TXN] = std::string(RW_BENCHMARK_W_TXN_NAME);
      txn_types[RW_BENCHMARK_R_TXN] = std::string(RW_BENCHMARK_R_TXN_NAME);
      break;
    case MICRO_BENCH:
      txn_types[MICRO_BENCH_R] = std::string(MICRO_BENCH_R_NAME);
      txn_types[MICRO_BENCH_W] = std::string(MICRO_BENCH_W_NAME);
      break;
    default:
      Log_fatal("benchmark not implemented");
      verify(0);
  }
}
//
//void TxnRequestFactory::init_txn_req(TxnRequest *req, uint32_t cid) {
//  if (txn_req_factory_s == NULL)
//    txn_req_factory_s = new TxnRequestFactory();
//  if (req)
//    return txn_req_factory_s->get_txn_req(req, cid);
//}
//
//void TxnRequestFactory::destroy() {
//  if (txn_req_factory_s != NULL) {
//    delete txn_req_factory_s;
//    txn_req_factory_s = NULL;
//  }
//}

TxnRequestFactory::~TxnRequestFactory() {
}

} // namespace rcc
