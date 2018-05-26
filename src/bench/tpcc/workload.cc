#include "workload.h"
#include <limits>

namespace janus {

char TPCC_TB_WAREHOUSE[] = "warehouse";
char TPCC_TB_DISTRICT[] = "district";
char TPCC_TB_CUSTOMER[] = "customer";
char TPCC_TB_HISTORY[] = "history";
char TPCC_TB_ORDER[] = "order";
char TPCC_TB_NEW_ORDER[] = "new_order";
char TPCC_TB_ITEM[] = "item";
char TPCC_TB_STOCK[] = "stock";
char TPCC_TB_ORDER_LINE[] = "order_line";
char TPCC_TB_ORDER_C_ID_SECONDARY[] = "order_secondary";

void TpccWorkload::RegisterPrecedures() {
  RegNewOrder();
  RegPayment();
  RegOrderStatus();
  RegDelivery();
  RegStockLevel();
}

TpccWorkload::~TpccWorkload() {}

TpccWorkload::TpccWorkload(Config *config)
    : Workload(config) {
  std::map<std::string, uint64_t> table_num_rows;
  sharding_->get_number_rows(table_num_rows);

  std::vector<unsigned int> partitions;
  sharding_->GetTablePartitions(TPCC_TB_WAREHOUSE, partitions);
  uint64_t tb_w_rows = table_num_rows[std::string(TPCC_TB_WAREHOUSE)];
  tpcc_para_.n_w_id_ = (int) tb_w_rows * partitions.size();
//  verify(tpcc_para_.n_w_id_ < 3);
  tpcc_para_.const_home_w_id_ =
      RandomGenerator::rand(0, tpcc_para_.n_w_id_ - 1);
  uint64_t tb_d_rows = table_num_rows[std::string(TPCC_TB_DISTRICT)];
  tpcc_para_.n_d_id_ = (int) tb_d_rows;
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


void TpccWorkload::GetNewOrderTxnReq(TxRequest *req,
                                     uint32_t cid) const {
  req->tx_type_ = TPCC_NEW_ORDER;
  //int home_w_id = RandomGenerator::rand(0, tpcc_para_.n_w_id_ - 1);
  int home_w_id = cid % tpcc_para_.n_w_id_;
  Value w_id((i32) home_w_id);
  //Value d_id((i32)RandomGenerator::rand(0, tpcc_para_.n_d_id_ - 1));
  Value d_id((i32) (cid / tpcc_para_.n_w_id_) % tpcc_para_.n_d_id_);
  Value c_id((i32) RandomGenerator::nu_rand(1022, 0, tpcc_para_.n_c_id_ - 1));
  int ol_cnt = RandomGenerator::rand(6, 15);
//  int ol_cnt = 0;

  rrr::i32 i_id_buf[ol_cnt];

  req->input_[TPCC_VAR_W_ID] = w_id;
  req->input_[TPCC_VAR_D_ID] = d_id;
  req->input_[TPCC_VAR_C_ID] = c_id;
  req->input_[TPCC_VAR_OL_CNT] = Value((i32) ol_cnt);
  req->input_[TPCC_VAR_O_CARRIER_ID] = Value((int32_t) 0);
  bool all_local = true;
  for (int i = 0; i < ol_cnt; i++) {
    //req->input_[4 + 3 * i] = Value((i32)RandomGenerator::nu_rand(8191, 0, tpcc_para_.n_i_id_ - 1)); XXX nurand is the standard
    rrr::i32
        tmp_i_id = (i32) RandomGenerator::rand(0, tpcc_para_.n_i_id_ - 1 - i);

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
      req->input_[TPCC_VAR_S_REMOTE_CNT(i)] = Value((i32) 1);
//      verify(req->input_[TPCC_VAR_S_W_ID(i)].get_i32() < 3);
      all_local = false;
    } else {
      req->input_[TPCC_VAR_S_W_ID(i)] = req->input_[TPCC_VAR_W_ID];
      req->input_[TPCC_VAR_S_REMOTE_CNT(i)] = Value((i32) 0);
//      verify(req->input_[TPCC_VAR_S_W_ID(i)].get_i32() < 3);
    }
    req->input_[TPCC_VAR_OL_QUANTITY(i)] =
        Value((i32) RandomGenerator::rand(0, 10));
  }
  req->input_[TPCC_VAR_O_ALL_LOCAL] =
      all_local ? Value((i32) 1) : Value((i32) 0);

}


void TpccWorkload::get_tpcc_payment_txn_req(
    TxRequest *req, uint32_t cid) const {
  req->tx_type_ = TPCC_PAYMENT;
  //int home_w_id = RandomGenerator::rand(0, tpcc_para_.n_w_id_ - 1);
  int home_w_id = cid % tpcc_para_.n_w_id_;
  Value c_w_id, c_d_id;
  Value w_id((i32) home_w_id);
  //Value d_id((i32)RandomGenerator::rand(0, tpcc_para_.n_d_id_ - 1));
  Value d_id((i32) (cid / tpcc_para_.n_w_id_) % tpcc_para_.n_d_id_);
  if (RandomGenerator::percentage_true(60)) { //XXX query by last name 60%
    req->input_[TPCC_VAR_C_LAST] =
        Value(RandomGenerator::int2str_n(RandomGenerator::nu_rand(255, 0, 999),
                                         3));;
  } else {
    req->input_[TPCC_VAR_C_ID] =
        Value((i32) RandomGenerator::nu_rand(1022, 0, tpcc_para_.n_c_id_ - 1));;
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
  req->input_[TPCC_VAR_C_W_ID] = c_w_id;
  req->input_[TPCC_VAR_C_D_ID] = c_d_id;
  req->input_[TPCC_VAR_H_AMOUNT] = h_amount;
  req->input_[TPCC_VAR_H_KEY] = Value((i32) RandomGenerator::rand()); // h_key
//  req->input_[TPCC_VAR_W_NAME] = Value();
//  req->input_[TPCC_VAR_D_NAME] = Value();
}

void TpccWorkload::get_tpcc_stock_level_txn_req(
    TxRequest *req, uint32_t cid) const {
  req->tx_type_ = TPCC_STOCK_LEVEL;
  req->input_[TPCC_VAR_W_ID] = Value((i32) (cid % tpcc_para_.n_w_id_));
  req->input_[TPCC_VAR_D_ID] =
      Value((i32) (cid / tpcc_para_.n_w_id_) % tpcc_para_.n_d_id_);
  req->input_[TPCC_VAR_THRESHOLD] = Value((i32) RandomGenerator::rand(10, 20));
}

void TpccWorkload::get_tpcc_delivery_txn_req(
    TxRequest *req, uint32_t cid) const {
  req->tx_type_ = TPCC_DELIVERY;
  req->input_[TPCC_VAR_W_ID] = Value((i32) (cid % tpcc_para_.n_w_id_));
  req->input_[TPCC_VAR_O_CARRIER_ID] =
      Value((i32) RandomGenerator::rand(1, 10));
  req->input_[TPCC_VAR_D_ID] =
      Value((i32) (cid / tpcc_para_.n_w_id_) % tpcc_para_.n_d_id_);
}

void TpccWorkload::get_tpcc_order_status_txn_req(
    TxRequest *req, uint32_t cid) const {
  req->tx_type_ = TPCC_ORDER_STATUS;
  if (RandomGenerator::percentage_true(60)) {//XXX 60% by c_last
    req->input_[TPCC_VAR_C_LAST] =
        Value(RandomGenerator::int2str_n(RandomGenerator::nu_rand(255, 0, 999),
                                         3));
  } else {
    req->input_[TPCC_VAR_C_ID] =
        Value((i32) RandomGenerator::nu_rand(1022, 0, tpcc_para_.n_c_id_ - 1));
  }
  req->input_[TPCC_VAR_W_ID] = Value((i32) (cid % tpcc_para_.n_w_id_));
  req->input_[TPCC_VAR_D_ID] =
      Value((i32) ((cid / tpcc_para_.n_w_id_) % tpcc_para_.n_d_id_));

}


void TpccWorkload::GetTxRequest(TxRequest* req, uint32_t cid) {
  req->n_try_ = n_try_;
  if (txn_weight_.size() != 5) {
    verify(0);
    GetNewOrderTxnReq(req, cid);
  } else {
    switch (RandomGenerator::weighted_select(txn_weight_)) {
      case 0:
        GetNewOrderTxnReq(req, cid);
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
}
