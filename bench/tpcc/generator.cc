
#include "generator.h"
#include "piece.h"
#include "deptran/txn_chopper.h"

namespace rococo {

TpccTxnGenerator::TpccTxnGenerator(Config* config)
    : TxnGenerator(config) {
  std::map<std::string, uint64_t> table_num_rows;
  sharding_->get_number_rows(table_num_rows);

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
}


void TpccTxnGenerator::get_tpcc_new_order_txn_req(
    TxnRequest *req, uint32_t cid) const {
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
//  req->input_.resize(4 + 3 * ol_cnt);
  //if (fix_id_ >= 0) {
  //    req->input_[0] = tpcc_para_.const_home_w_id_;
  //    req->input_[1] = fix_id_;
  //}
  //else {
  req->input_[TPCC_VAR_W_ID] = w_id;
  req->input_[TPCC_VAR_D_ID] = d_id;
  //}
  req->input_[TPCC_VAR_C_ID] = c_id;
  req->input_[TPCC_VAR_OL_CNT] = Value((i32) ol_cnt);
  req->input_[TPCC_VAR_O_CARRIER_ID] = Value((int32_t)0);
  bool all_local = true;
  for (int i = 0; i < ol_cnt; i++) {
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
    req->input_[TPCC_VAR_I_ID(i)] = Value(tmp_i_id);
    req->input_[TPCC_VAR_OL_NUMBER(i)] = i;
    req->input_[TPCC_VAR_OL_DELIVER_D(i)] = std::string();
    req->input_[TPCC_VAR_OL_DIST_INFO(i)] = std::string();

    if (tpcc_para_.n_w_id_ > 1 && // warehouse more than one, can do remote
        RandomGenerator::percentage_true(1)) { //XXX 1% REMOTE_RATIO
      int remote_w_id = RandomGenerator::rand(0, tpcc_para_.n_w_id_ - 2);
      remote_w_id = remote_w_id >= home_w_id ? remote_w_id + 1 : remote_w_id;
      req->input_[TPCC_VAR_S_W_ID(i)] = Value((i32) remote_w_id);
      req->input_[TPCC_VAR_S_REMOTE_CNT(i)] = Value((i32)1);
      all_local = false;
    } else {
      req->input_[TPCC_VAR_S_W_ID(i)] = req->input_[TPCC_VAR_W_ID];
      req->input_[TPCC_VAR_S_REMOTE_CNT(i)] = Value((i32)0);
    }
    req->input_[TPCC_VAR_OL_QUANTITY(i)] = Value((i32) RandomGenerator::rand(0, 10));
  }
  req->input_[TPCC_VAR_O_ALL_LOCAL] = all_local ? Value((i32)1) : Value ((i32)0);


  //for (i = 0; i < ol_cnt; i++) {
  //    verify(i_id_buf[i] < tpcc_para_.n_i_id_ && i_id_buf[i] >= 0);
  //    for (int j = i + 1; j < ol_cnt; j++)
  //        verify(i_id_buf[i] != i_id_buf[j]);
  //}

}


void TpccTxnGenerator::get_tpcc_payment_txn_req(
    TxnRequest *req, uint32_t cid) const {
  req->txn_type_ = TPCC_PAYMENT;
  //int home_w_id = RandomGenerator::rand(0, tpcc_para_.n_w_id_ - 1);
  int home_w_id = cid % tpcc_para_.n_w_id_;
  Value c_w_id, c_d_id;
  Value w_id((i32) home_w_id);
  //Value d_id((i32)RandomGenerator::rand(0, tpcc_para_.n_d_id_ - 1));
  Value d_id((i32) (cid / tpcc_para_.n_w_id_) % tpcc_para_.n_d_id_);
  Value c_id_or_last;
  if (RandomGenerator::percentage_true(60)) { //XXX query by last name 60%
    c_id_or_last = Value(RandomGenerator::int2str_n(RandomGenerator::nu_rand(255, 0, 999), 3));
    req->input_[TPCC_VAR_C_LAST] = c_id_or_last;
  } else {
    c_id_or_last = Value((i32) RandomGenerator::nu_rand(1022, 0, tpcc_para_.n_c_id_ - 1));
    req->input_[TPCC_VAR_C_ID] = c_id_or_last;
  }
  if (tpcc_para_.n_w_id_ > 1 && // warehouse more than one, can do remote
      RandomGenerator::percentage_true(15)) { //XXX 15% pay through remote warehouse, 85 home REMOTE_RATIO
    int c_w_id_int = RandomGenerator::rand(0, tpcc_para_.n_w_id_ - 2);
    c_w_id_int = c_w_id_int >= home_w_id ? c_w_id_int + 1 : c_w_id_int;
    c_w_id = Value((i32) c_w_id_int);
    c_d_id = Value((i32) RandomGenerator::rand(0, tpcc_para_.n_d_id_ - 1));
  } else {
    c_w_id = w_id;
    c_d_id = d_id;
  }
  Value h_amount(RandomGenerator::rand_double(1.00, 5000.00));
//  req->input_.resize(7);
  req->input_[TPCC_VAR_W_ID] = w_id;
  req->input_[TPCC_VAR_D_ID] = d_id;
//  req->input_[TPCC_VAR_C_ID_LAST] = c_id_or_last;
  req->input_[TPCC_VAR_C_W_ID] = c_w_id;
  req->input_[TPCC_VAR_C_D_ID] = c_d_id;
  req->input_[TPCC_VAR_H_AMOUNT] = h_amount;
  req->input_[TPCC_VAR_H_KEY] = Value((i32) RandomGenerator::rand()); // h_key
//  req->input_[TPCC_VAR_W_NAME] = Value();
//  req->input_[TPCC_VAR_D_NAME] = Value();
}

void TpccTxnGenerator::get_tpcc_stock_level_txn_req(
    TxnRequest *req, uint32_t cid) const {
  req->txn_type_ = TPCC_STOCK_LEVEL;
//  req->input_.resize(3);
  //req->input_[0] = Value((i32)RandomGenerator::rand(0, tpcc_para_.n_w_id_ - 1));
  //req->input_[1] = Value((i32)RandomGenerator::rand(0, tpcc_para_.n_d_id_ - 1));
  req->input_[TPCC_VAR_W_ID] = Value((i32) (cid % tpcc_para_.n_w_id_));
  req->input_[TPCC_VAR_D_ID] = Value((i32) (cid / tpcc_para_.n_w_id_) % tpcc_para_.n_d_id_);
  req->input_[TPCC_VAR_THRESHOLD] = Value((i32) RandomGenerator::rand(10, 20));
}

void TpccTxnGenerator::get_tpcc_delivery_txn_req(
    TxnRequest *req, uint32_t cid) const {
  req->txn_type_ = TPCC_DELIVERY;
//  req->input_.resize(3);
  //req->input_[0] = Value((i32)RandomGenerator::rand(0, tpcc_para_.n_w_id_ - 1));
  req->input_[TPCC_VAR_W_ID] = Value((i32) (cid % tpcc_para_.n_w_id_));
  req->input_[TPCC_VAR_O_CARRIER_ID] = Value((i32) RandomGenerator::rand(1, 10));
  //req->input_[2] = Value((i32)tpcc_para_.delivery_d_id_++);
  //if (tpcc_para_.delivery_d_id_ >= tpcc_para_.n_d_id_)
  //    tpcc_para_.delivery_d_id_ = 0;
  req->input_[TPCC_VAR_D_ID] = Value((i32) (cid / tpcc_para_.n_w_id_) % tpcc_para_.n_d_id_);
}

void TpccTxnGenerator::get_tpcc_order_status_txn_req(
    TxnRequest *req, uint32_t cid) const {
  req->txn_type_ = TPCC_ORDER_STATUS;
  Value c_id_or_last;
  if (RandomGenerator::percentage_true(60)) //XXX 60% by c_last
    c_id_or_last = Value(RandomGenerator::int2str_n(RandomGenerator::nu_rand(255, 0, 999), 3));
  else
    c_id_or_last = Value((i32) RandomGenerator::nu_rand(1022, 0, tpcc_para_.n_c_id_ - 1));
//  req->input_.resize(3);
  //req->input_[0] = Value((i32)RandomGenerator::rand(0, tpcc_para_.n_w_id_ - 1));
  //req->input_[1] = Value((i32)RandomGenerator::rand(0, tpcc_para_.n_d_id_ - 1));
  req->input_[TPCC_VAR_W_ID] = Value((i32) (cid % tpcc_para_.n_w_id_));
  req->input_[TPCC_VAR_D_ID] = Value((i32) ((cid / tpcc_para_.n_w_id_) % tpcc_para_.n_d_id_));
  req->input_[TPCC_VAR_C_ID_LAST] = c_id_or_last;
}


void TpccTxnGenerator::get_txn_req(TxnRequest *req, uint32_t cid) const {
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

} // namespace