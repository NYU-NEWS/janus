#include "all.h"

namespace rococo {
#define TPCC_NEW_ORDER_Ith_INDEX_ITEM(i)        (5 + 4 * i)
#define TPCC_NEW_ORDER_Ith_INDEX_IM_STOCK(i)    (6 + 4 * i)
#define TPCC_NEW_ORDER_Ith_INDEX_DEFER_STOCK(i) (7 + 4 * i)
#define TPCC_NEW_ORDER_Ith_INDEX_ORDER_LINE(i)  (8 + 4 * i)
#define TPCC_NEW_ORDER_IS_ITEM_INDEX(i)         ((i >= 5) && (i % 4) == 1)
#define TPCC_NEW_ORDER_IS_IM_STOCK_INDEX(i)     ((i >= 5) && (i % 4) == 2)
#define TPCC_NEW_ORDER_INDEX_ITEM_TO_ORDER_LINE(i)  (i + 3)
#define TPCC_NEW_ORDER_INDEX_ITEM_TO_DEFER_STOCK(i) (i + 2)
#define TPCC_NEW_ORDER_INDEX_IM_STOCK_TO_ORDER_LINE(i) (i + 2)
#define TPCC_NEW_ORDER_INDEX_ITEM_TO_CNT(i)     ((i - 5) / 4)
#define TPCC_NEW_ORDER_INDEX_IM_STOCK_TO_CNT(i) ((i - 6) / 4)

#define TPCC_DELIVERY_Ith_INDEX_NEW_ORDER(i)    (4 * i)
#define TPCC_DELIVERY_Ith_INDEX_ORDER(i)        (1 + 4 * i)
#define TPCC_DELIVERY_Ith_INDEX_ORDER_LINE(i)   (2 + 4 * i)
#define TPCC_DELIVERY_Ith_INDEX_CUSTOMER(i)     (3 + 4 * i)
#define TPCC_DELIVERY_IS_NEW_ORDER_INDEX(i)     ((i % 4) == 0)
#define TPCC_DELIVERY_IS_ORDER_INDEX(i)         ((i % 4) == 1)
#define TPCC_DELIVERY_IS_ORDER_LINE_INDEX(i)    ((i % 4) == 2)
#define TPCC_DELIVERY_IS_CUSTOMER(i)            ((i % 4) == 3)
#define TPCC_DELIVERY_INDEX_NEW_ORDER_TO_CNT(i)         (i / 4)
#define TPCC_DELIVERY_INDEX_NEW_ORDER_TO_ORDER(i)       (i + 1)
#define TPCC_DELIVERY_INDEX_NEW_ORDER_TO_ORDER_LINE(i)  (i + 2)
#define TPCC_DELIVERY_INDEX_NEW_ORDER_TO_CUSTOMER(i)    (i + 3)
#define TPCC_DELIVERY_INDEX_ORDER_TO_CNT(i)             ((i - 1) / 4)
#define TPCC_DELIVERY_INDEX_ORDER_TO_CUSTOMER(i)        (i + 2)
#define TPCC_DELIVERY_INDEX_ORDER_LINE_TO_CNT(i)        ((i - 2) / 4)
#define TPCC_DELIVERY_INDEX_ORDER_LINE_TO_CUSTOMER(i)   (i + 1)

TpccChopper::TpccChopper() {
}

void TpccChopper::init(TxnRequest &req) {
  txn_type_ = req.txn_type_;
  callback_ = req.callback_;
  max_try_ = req.n_try_;
  n_try_ = 1;
  commit_.store(true);

  switch (txn_type_) {
    case TPCC_NEW_ORDER:
      new_order_init(req);
      break;
    case TPCC_PAYMENT:
      payment_init(req);
      break;
    case TPCC_ORDER_STATUS:
      order_status_init(req);
      break;
    case TPCC_DELIVERY:
      delivery_init(req);
      break;
    case TPCC_STOCK_LEVEL:
      stock_level_init(req);
      break;
    default:
      verify(0);
  }
}

void TpccChopper::new_order_shard(const char *tb,
                                  const std::vector<Value> &input,
                                  uint32_t &site,
                                  int cnt) {

  // partition based on w_id
  MultiValue mv;
  if (tb == TPCC_TB_DISTRICT
      || tb == TPCC_TB_WAREHOUSE
      || tb == TPCC_TB_CUSTOMER
      || tb == TPCC_TB_ORDER
      || tb == TPCC_TB_NEW_ORDER
      || tb == TPCC_TB_ORDER_LINE)
    mv = MultiValue(input[0]);
  else if (tb == TPCC_TB_ITEM)
    // based on i_id
    mv = MultiValue(input[4 + 3 * cnt]);
  else if (tb == TPCC_TB_STOCK)
    // based on s_w_id
    mv = MultiValue(input[5 + 3 * cnt]);
  else
    verify(0);
  int ret = sss_->get_site_id_from_tb(tb, mv, site);
  verify(ret == 0);
}

void TpccChopper::new_order_init(TxnRequest &req) {
  new_order_dep_ = new_order_dep_t();
  /**
   * req.input_
   *  0       ==> w_id, d_w_id, c_w_id
   *  1       ==> d_id, c_d_id
   *  2       ==> c_id
   *  3       ==> ol_cnt
   *  4 + 3*i ==> s_i_id, ol_i_id, i_id
   *  5 + 3*i ==> ol_supply_w_id
   *  6 + 3*i ==> ol_quantity
   **/
  int32_t ol_cnt = req.input_[3].get_i32();

  new_order_dep_.piece_0_dist = false;
  new_order_dep_.ol_cnt = (size_t) ol_cnt;
  new_order_dep_.piece_items = (bool *) malloc(sizeof(bool) * ol_cnt);
  new_order_dep_.piece_stocks = (bool *) malloc(sizeof(bool) * ol_cnt);
  memset(new_order_dep_.piece_items, false, sizeof(bool) * ol_cnt);
  memset(new_order_dep_.piece_stocks, false, sizeof(bool) * ol_cnt);

  n_pieces_all_ = 5 + 4 * ol_cnt;
//  inputs_.resize(n_pieces_all_);
  output_size_.resize(n_pieces_all_);
  p_types_.resize(n_pieces_all_);
  sharding_.resize(n_pieces_all_);
  status_.resize(n_pieces_all_);

  // piece 0, Ri&W district
  inputs_[0] = map<int32_t, Value>({
                                       {0, req.input_[0]},  // 0 ==> d_w_id
                                       {1, req.input_[1]}   // 1 ==> d_id
                                   });
  output_size_[0] = 2;
  p_types_[0] = TPCC_NEW_ORDER_0;
  new_order_shard(TPCC_TB_DISTRICT, req.input_,  // sharding based on d_w_id
                  sharding_[0]);
  status_[0] = READY;

  // piece 1, R warehouse
  inputs_[1] = map<int32_t, Value>({
                                       {0, req.input_[0]}   // 0 ==> w_id
                                   });
  output_size_[1] = 1;
  p_types_[1] = TPCC_NEW_ORDER_1;
  new_order_shard(TPCC_TB_WAREHOUSE, req.input_,  // sharding based on w_id
                  sharding_[1]);
  status_[1] = READY;

  // piece 2, R customer
  inputs_[2] = map<int32_t, Value>({
                                       {0, req.input_[0]},  // 0 ==> c_w_id
                                       {1, req.input_[1]},  // 1 ==> c_d_id
                                       {2, req.input_[2]}   // 2 ==> c_id
                                   });
  output_size_[2] = 3;
  p_types_[2] = TPCC_NEW_ORDER_2;
  new_order_shard(TPCC_TB_CUSTOMER, req.input_,  // sharding based on c_w_id
                  sharding_[2]);
  status_[2] = READY;

  bool all_local = true;
  for (int i = 0; i < ol_cnt; i++) {
    Value is_remote((i32) 0);
    if (req.input_[5 + 3 * i] != req.input_[0]) {
      all_local = false;
      is_remote.set_i32((i32) 1);
    }
    // piece 5 + 4 * i, Ri item
    inputs_[TPCC_NEW_ORDER_Ith_INDEX_ITEM(i)] =
        map<int32_t, Value>({
                                {0, req.input_[4 + 3 * i]}   // 0 ==> i_id
                            });
    output_size_[TPCC_NEW_ORDER_Ith_INDEX_ITEM(i)] = 3;
    p_types_[TPCC_NEW_ORDER_Ith_INDEX_ITEM(i)] = TPCC_NEW_ORDER_5;
    new_order_shard(TPCC_TB_ITEM,
                    req.input_,
                    sharding_[TPCC_NEW_ORDER_Ith_INDEX_ITEM(i)],
                    i);
    status_[TPCC_NEW_ORDER_Ith_INDEX_ITEM(i)] = READY;

    // piece 6 + 4 * i, Ri stock
    inputs_[TPCC_NEW_ORDER_Ith_INDEX_IM_STOCK(i)] =
        map<int32_t, Value>({
                                {0, req.input_[4 + 3 * i]},  // 0 ==> s_i_id
                                {1, req.input_[5 + 3 * i]},  // 1 ==>
                                // ol_supply_w_id / s_w_id
                                {2, req.input_[1]}           // 2 ==> d_id
                            });
    output_size_[TPCC_NEW_ORDER_Ith_INDEX_IM_STOCK(i)] = 2;
    p_types_[TPCC_NEW_ORDER_Ith_INDEX_IM_STOCK(i)] = TPCC_NEW_ORDER_6;
    new_order_shard(TPCC_TB_STOCK, req.input_,
                    sharding_[TPCC_NEW_ORDER_Ith_INDEX_IM_STOCK(i)], i);
    status_[TPCC_NEW_ORDER_Ith_INDEX_IM_STOCK(i)] = READY;

    // piece 7 + 4 * i, W stock
    inputs_[TPCC_NEW_ORDER_Ith_INDEX_DEFER_STOCK(i)] =
        map<int32_t, Value>({
                                {0, req.input_[4 + 3 * i]},  // 0 ==> s_i_id
                                {1, req.input_[5 + 3 * i]},  // 1 ==>
                                // ol_supply_w_id / s_w_id
                                {2, req.input_[6 + 3 * i]},  // 2 ==>
                                // ol_quantity
                                {3, is_remote}               // 3 ==> increase
                                    // delta s_remote_cnt
                            });
    output_size_[TPCC_NEW_ORDER_Ith_INDEX_DEFER_STOCK(i)] = 0;
    p_types_[TPCC_NEW_ORDER_Ith_INDEX_DEFER_STOCK(i)] = TPCC_NEW_ORDER_7;
    new_order_shard(TPCC_TB_STOCK, req.input_,
                    sharding_[TPCC_NEW_ORDER_Ith_INDEX_DEFER_STOCK(i)], i);
    status_[TPCC_NEW_ORDER_Ith_INDEX_DEFER_STOCK(i)] = READY;

    // piece 8 + 4 * i, W order_line, depends on piece 0 & 5+3*i & 6+3*i
    inputs_[TPCC_NEW_ORDER_Ith_INDEX_ORDER_LINE(i)] =
        map<int32_t, Value>({
            {0, req.input_[1]},          // 0 ==> ol_d_id
            {1, req.input_[0]},          // 1 ==> ol_w_id
            {2, Value((i32) 0)},          // 2 ==> ol_o_id    depends on piece 0
            {3, Value((i32) i)},          // 3 ==> ol_number
            {4, req.input_[4 + 3 * i]},  // 4 ==> ol_i_id
            {5, req.input_[5 + 3 * i]},  // 5 ==> ol_supply_w_id
            {6, Value(std::string())},   // 6 ==> ol_deliver_d
            {7, req.input_[6 + 3 * i]},  // 7 ==> ol_quantity
            {8, Value((double) 0.0)},     // 8 ==> ol_amount  depends on piece 5+3*i
            {9, Value(std::string())},   // 9 ==> ol_dist_info depends on piece 6+3*i
                            });
    output_size_[TPCC_NEW_ORDER_Ith_INDEX_ORDER_LINE(i)] = 0;
    p_types_[TPCC_NEW_ORDER_Ith_INDEX_ORDER_LINE(i)] = TPCC_NEW_ORDER_8;
    new_order_shard(TPCC_TB_ORDER_LINE, req.input_,// sharding based on ol_w_id
                    sharding_[TPCC_NEW_ORDER_Ith_INDEX_ORDER_LINE(i)]);
    status_[TPCC_NEW_ORDER_Ith_INDEX_ORDER_LINE(i)] = WAITING;
  }
  // piece 3, W order, depends on piece 0
  inputs_[3] =
      map<int32_t, Value>({
                              {0, Value((i32) 0)},
                              // 0 ==> // o_id   depends on piece 0
                              {1, req.input_[1]},
                              // 1 ==> // o_d_id
                              {2, req.input_[0]},
                              // 2 ==> // o_w_id
                              {3, req.input_[2]},
                              // 3 ==> // o_c_id
                              {4, Value((i32) 0)},
                              // 4 ==> // o_carrier_id
                              {5, req.input_[3]},
                              // 5 ==> // o_ol_cnt
                              {6, all_local ?
                                  Value((i32) 1) : Value((i32) 0)
                              }// 6 ==> // o_all_local
                          });
  output_size_[3] = 0;
  p_types_[3] = TPCC_NEW_ORDER_3;
  new_order_shard(TPCC_TB_ORDER, req.input_, // sharding based on o_w_id
                  sharding_[3]);
  status_[3] = WAITING;

  // piece 4, W new_order, depends on piece 0
  inputs_[4] = map<int32_t, Value>({
      {0, Value((i32) 0)},          // 0 ==> no_id   depends on piece 0
      {1, req.input_[1]},          // 1 ==> no_d_id
      {2, req.input_[0]},          // 2 ==> no_w_id
                                   });
  output_size_[4] = 0;
  p_types_[4] = TPCC_NEW_ORDER_4;
  new_order_shard(TPCC_TB_NEW_ORDER, req.input_, // sharding based on no_w_id
                  sharding_[4]);
  status_[4] = WAITING;
}

bool TpccChopper::new_order_callback(int pi,
                                     int res,
                                     const Value *output,
                                     uint32_t output_size) {

  if (pi == 0) { // piece 0 started, set piece 3, piece 4, piece 8+4i.0
    new_order_dep_.piece_0_dist = true;
    Value o_id = output[1];
    inputs_[3][0] = o_id;
    status_[3] = READY;
    inputs_[4][0] = o_id;
    status_[4] = READY;

    for (size_t i = 0; i < new_order_dep_.ol_cnt; i++) {
      inputs_[TPCC_NEW_ORDER_Ith_INDEX_ORDER_LINE(i)][2] = o_id;
      if (new_order_dep_.piece_items[i] && new_order_dep_.piece_stocks[i])
        status_[TPCC_NEW_ORDER_Ith_INDEX_ORDER_LINE(i)] = READY;
    }

    return true;
  }
  else if (TPCC_NEW_ORDER_IS_ITEM_INDEX(pi)) { // piece 5+4i started, set piece 8+4i.8
    inputs_[TPCC_NEW_ORDER_INDEX_ITEM_TO_ORDER_LINE(pi)][8]
        = Value((double) (output[1].get_double()
        * inputs_[TPCC_NEW_ORDER_INDEX_ITEM_TO_DEFER_STOCK(pi)][2].get_i32()));
    new_order_dep_.piece_items[TPCC_NEW_ORDER_INDEX_ITEM_TO_CNT(pi)] = true;
    if (new_order_dep_.piece_0_dist
        && new_order_dep_.piece_stocks[TPCC_NEW_ORDER_INDEX_ITEM_TO_CNT(pi)]) {
      status_[TPCC_NEW_ORDER_INDEX_ITEM_TO_ORDER_LINE(pi)] = READY;
      return true;
    }
    else
      return false;
  }
  else if (TPCC_NEW_ORDER_IS_IM_STOCK_INDEX(pi)) { // piece 6+4i started, set piece 8+4i.9
    inputs_[TPCC_NEW_ORDER_INDEX_IM_STOCK_TO_ORDER_LINE(pi)][9] = output[0];
    new_order_dep_.piece_stocks[TPCC_NEW_ORDER_INDEX_IM_STOCK_TO_CNT(pi)] =
        true;
    if (new_order_dep_.piece_0_dist
        && new_order_dep_.piece_items[TPCC_NEW_ORDER_INDEX_IM_STOCK_TO_CNT(pi)]) {
      status_[TPCC_NEW_ORDER_INDEX_IM_STOCK_TO_ORDER_LINE(pi)] = READY;
      return true;
    }
    else
      return false;
  }
  else
    return false;
}

bool TpccChopper::start_callback(int pi,
                                 int res,
                                 map<int32_t, Value> &output_map) {
  vector<Value> output;
  for (auto &kv : output_map) {
    output.push_back(kv.second);
  }

  PieceCallbackHandler handler;
  auto it = txn_reg_->callbacks_.find(std::make_pair(txn_type_, pi));
  if (it != txn_reg_->callbacks_.end()) {
    handler = it->second;
  } else {
    handler = [] (TxnChopper* ch,
                  map<int32_t, Value>& output) -> bool { return false; };
  }

  switch (txn_type_) {
    case TPCC_NEW_ORDER:
      return new_order_callback(pi, res, output.data(), output.size());
    case TPCC_STOCK_LEVEL:
    case TPCC_DELIVERY:
    case TPCC_PAYMENT: {
      return handler(this, output_map);
    }
    case TPCC_ORDER_STATUS:
      return order_status_callback(pi, res, output.data(), output.size());
//      return delivery_callback(pi, res, output.data(), output.size());
//      return stock_level_callback(pi, res, output.data(), output.size());
    default:
      verify(0);
  }
  return false;
}

bool TpccChopper::start_callback(const std::vector<int> &pi,
                                 int res,
                                 BatchStartArgsHelper &bsah) {
  verify(0);
  bool ret = false;
//  bool (TpccChopper::*callback_func)(int, int, const Value *, uint32_t);
//  switch (txn_type_) {
//    case TPCC_NEW_ORDER:
//      callback_func = &TpccChopper::new_order_callback;
//      break;
//    case TPCC_PAYMENT:
//      callback_func = &TpccChopper::payment_callback;
//      break;
//    case TPCC_ORDER_STATUS:
//      callback_func = &TpccChopper::order_status_callback;
//      break;
//    case TPCC_DELIVERY:
//      callback_func = &TpccChopper::delivery_callback;
//      break;
//    case TPCC_STOCK_LEVEL:
//      callback_func = &TpccChopper::stock_level_callback;
//      break;
//    default:
//      verify(0);
//  }
//
//  rrr::i32 res_buf;
//  const Value *output;
//  uint32_t output_size;
//  int i = 0;
//  while (0 == bsah.get_next_output_client(&res_buf, &output, &output_size)) {
//    if ((this->*callback_func)(pi[i], res_buf, output, output_size))
//      ret = true;
//    i++;
//  }

  return ret;
}

void TpccChopper::new_order_retry() {
  status_[0] = READY;
  status_[1] = READY;
  status_[2] = READY;
  status_[3] = WAITING;
  status_[4] = WAITING;
  new_order_dep_.piece_0_dist = false;
  for (size_t i = 0; i < new_order_dep_.ol_cnt; i++) {
    new_order_dep_.piece_items[i] = false;
    new_order_dep_.piece_stocks[i] = false;
    status_[TPCC_NEW_ORDER_Ith_INDEX_ITEM(i)] = READY;
    status_[TPCC_NEW_ORDER_Ith_INDEX_IM_STOCK(i)] = READY;
    status_[TPCC_NEW_ORDER_Ith_INDEX_DEFER_STOCK(i)] = READY;
    status_[TPCC_NEW_ORDER_Ith_INDEX_ORDER_LINE(i)] = WAITING;
  }
}

void TpccChopper::payment_shard(const char *tb,
                                const std::vector<mdb::Value> &input,
                                uint32_t &site) {
  MultiValue mv;
  if (tb == TPCC_TB_WAREHOUSE
      || tb == TPCC_TB_DISTRICT)
    mv = MultiValue(input[0]);
  else if (tb == TPCC_TB_CUSTOMER)
    mv = MultiValue(input[3]);
  else if (tb == TPCC_TB_HISTORY)
    mv = MultiValue(input[6]);
  else
    verify(0);
  int ret = sss_->get_site_id_from_tb(tb, mv, site);
  verify(ret == 0);
}

void TpccChopper::payment_init(TxnRequest &req) {
  payment_dep_.piece_warehouse = false;
  payment_dep_.piece_district = false;
  /**
   * req.input_
   *  0       ==> w_id, d_w_id
   *  1       ==> d_id
   *  2       ==> c_id or last_name
   *  3       ==> c_w_id
   *  4       ==> c_d_id
   *  5       ==> h_amount
   *  6       ==> h_key
   **/

  n_pieces_all_ = 6;
//  inputs_.resize(n_pieces_all_);
  output_size_.resize(n_pieces_all_);
  p_types_.resize(n_pieces_all_);
  sharding_.resize(n_pieces_all_);
  status_.resize(n_pieces_all_);

  // piece 0, Ri
  inputs_[0] = map<int32_t, Value>({
                                       {0, req.input_[0]},  // 0 ==>    w_id
                                       {1, req.input_[5]}  // 1 ==>    h_amount
                                   });
  output_size_[0] = 6;
  p_types_[0] = TPCC_PAYMENT_0;
  payment_shard(TPCC_TB_WAREHOUSE, req.input_, sharding_[0]);
  status_[0] = READY;

  // piece 1, Ri district
  inputs_[1] = map<int32_t, Value>({
                                       {0, req.input_[0]},  // 0 ==>    d_w_id
                                       {1, req.input_[1]},  // 1 ==>    d_id
                                   });
  output_size_[1] = 6;
  p_types_[1] = TPCC_PAYMENT_1;
  payment_shard(TPCC_TB_DISTRICT, req.input_, sharding_[1]);
  status_[1] = READY;

  // piece 2, W district
  inputs_[2] = map<int32_t, Value>({
                                       {0, req.input_[0]},
                                       // 0 ==>    d_w_id
                                       {1, req.input_[1]},
                                       // 1 ==>    d_id
                                       {2, req.input_[5]}
                                       // 2 ==>    h_amount
                                   });
  output_size_[2] = 0;
  p_types_[2] = TPCC_PAYMENT_2;
  payment_shard(TPCC_TB_DISTRICT, req.input_, sharding_[2]);
  status_[2] = READY;

  // query by c_last
  if (req.input_[2].get_kind() == mdb::Value::STR) {
    // piece 3, R customer, c_last -> c_id
    inputs_[3] = map<int32_t, Value>({
        {0, req.input_[2]},  // 0 ==>    c_last
        {1, req.input_[3]},  // 1 ==>    c_w_id
        {2, req.input_[4]},  // 2 ==>    c_d_id
                                     });
    output_size_[3] = 1;
    p_types_[3] = TPCC_PAYMENT_3;
    payment_shard(TPCC_TB_CUSTOMER, req.input_, sharding_[3]);
    status_[3] = READY;
    // piece 4, set it to waiting
    status_[4] = WAITING;

    payment_dep_.piece_last2id = false;
    payment_dep_.piece_ori_last2id = false;
  }
    // query by c_id, invalidate piece 2
  else {
    Log_debug("payment transaction lookup by customer name");
//    verify(0);
    // piece 3, R customer, set it to finish
    status_[3] = FINISHED;
    // piece 4, set it to ready
    status_[4] = READY;
    n_pieces_out_ = 1;

    payment_dep_.piece_last2id = true;
    payment_dep_.piece_ori_last2id = true;
  }

  // piece 4, R & W customer
  inputs_[4] = map<int32_t, Value>({
      {0, req.input_[2]},  // 0 ==>    c_id
      {1, req.input_[3]},  // 1 ==>    c_w_id
      {2, req.input_[4]},  // 2 ==>    c_d_id
      {3, req.input_[5]},  // 3 ==>    h_amount
      {4, req.input_[0]},  // 4 ==>    w_id
      {5, req.input_[1]}   // 5 ==>    d_id
                                   });
  output_size_[4] = 15;
  p_types_[4] = TPCC_PAYMENT_4;
  payment_shard(TPCC_TB_CUSTOMER, req.input_, sharding_[4]);

  // piece 5, W history (insert), depends on piece 0, 1
  inputs_[5] = map<int32_t, Value>({
      {0, req.input_[6]},  // 0 ==>    h_key
      {1, Value()},        // 1 ==>    w_name depends on piece 0
      {2, Value()},        // 2 ==>    d_name depends on piece 1
      {3, req.input_[0]},  // 3 ==>    w_id
      {4, req.input_[1]},  // 4 ==>    d_id
      {5, req.input_[2]},  // 5 ==>    c_id depends on piece 2 if querying by c_last
      {6, req.input_[3]},  // 6 ==>    c_w_id
      {7, req.input_[4]},  // 7 ==>    c_d_id
      {8, req.input_[5]}   // 8 ==>    h_amount
                                   });
  output_size_[5] = 0;
  p_types_[5] = TPCC_PAYMENT_5;
  payment_shard(TPCC_TB_HISTORY, req.input_, sharding_[5]);
  status_[5] = WAITING;
}

bool TpccChopper::payment_callback(int pi,
                                   int res,
                                   const Value *output,
                                   uint32_t output_size) {
  verify(0);
//  Log_debug("pi: %d", pi);
//  if (pi == 0) {
//    verify(!payment_dep_.piece_warehouse);
//    verify(output_size == 6);
//    payment_dep_.piece_warehouse = true;
//    inputs_[5][1] = output[0];
//    if (payment_dep_.piece_district && payment_dep_.piece_last2id) {
//      Log_debug("warehouse: d_name c_id ready");
//      status_[5] = READY;
//      return true;
//    }
//    Log_debug("warehouse: d_name c_id not ready");
//    return false;
//  } else if (pi == 1) {
//    verify(!payment_dep_.piece_district);
//    verify(output_size == 6);
//    payment_dep_.piece_district = true;
//    inputs_[5][2] = output[0];
//    if (payment_dep_.piece_warehouse && payment_dep_.piece_last2id) {
//      Log_debug("warehouse: w_name c_id ready");
//      status_[5] = READY;
//      return true;
//    }
//    Log_debug("warehouse: w_name c_id not ready");
//    return false;
//  } else if (pi == 2) {
//    return false;
//  } else if (pi == 3) {
//    verify(!payment_dep_.piece_last2id);
//    verify(output_size == 1);
//    payment_dep_.piece_last2id = true;
//    // set piece 4 ready
//    inputs_[4][0] = output[0];
//    status_[4] = READY;
//
//    inputs_[5][5] = output[0];
//    if (payment_dep_.piece_warehouse && payment_dep_.piece_district) {
//      status_[5] = READY;
//      Log_debug("customer: w_name c_id not ready");
//    }
//    return true;
//  } else if (pi == 4) {
//    verify(output_size == 15);
//    if (!payment_dep_.piece_ori_last2id) {
//      verify(output[3].get_str() == inputs_[3][0].get_str());
//    }
//    return false;
//  } else if (pi == 5) {
//    return false;
//  } else
//    verify(0);
}

void TpccChopper::payment_retry() {
  status_[0] = READY;
  status_[1] = READY;
  status_[2] = READY;
  // query by c_id
  if (payment_dep_.piece_ori_last2id) {
    status_[3] = FINISHED;
    status_[4] = READY;
    n_pieces_out_ = 1;
  }
    // query by c_last
  else {
    status_[3] = READY;
    status_[4] = WAITING;
  }
  status_[5] = WAITING;
  payment_dep_.piece_warehouse = false;
  payment_dep_.piece_district = false;
  payment_dep_.piece_last2id = payment_dep_.piece_ori_last2id;
}

void TpccChopper::order_status_shard(const char *tb,
                                     const std::vector<mdb::Value> &input,
                                     uint32_t &site) {
  MultiValue mv;
  if (tb == TPCC_TB_CUSTOMER
      || tb == TPCC_TB_ORDER
      || tb == TPCC_TB_ORDER_LINE) {
    mv = MultiValue(input[0]);
  }
  else
    verify(0);
  int ret = sss_->get_site_id_from_tb(tb, mv, site);
  verify(ret == 0);
}

void TpccChopper::order_status_init(TxnRequest &req) {
  order_status_dep_.piece_order = false;
  /**
   * req.input_
   *  0       ==> w_id
   *  1       ==> d_id
   *  2       ==> c_id or last_name
   **/

  n_pieces_all_ = 4;
//  inputs_.resize(n_pieces_all_);
  output_size_.resize(n_pieces_all_);
  p_types_.resize(n_pieces_all_);
  sharding_.resize(n_pieces_all_);
  status_.resize(n_pieces_all_);

  if (req.input_[2].get_kind() == Value::I32) { // query by c_id
    status_[0] = FINISHED; // piece 0 not needed
    status_[1] = READY; // piece 1 ready
    status_[2] = READY; // piece 2 ready

    order_status_dep_.piece_last2id = true;
    order_status_dep_.piece_ori_last2id = true;

    n_pieces_out_ = 1; // since piece 0 not needed, set it as one started piece
  }
  else { // query by c_last

    // piece 0, R customer, c_last --> c_id
    inputs_[0] = map<int32_t, Value>({
        {0, req.input_[2]},  // 0 ==>    c_last
        {1, req.input_[0]},  // 1 ==>    c_w_id
        {2, req.input_[1]}   // 2 ==>    c_d_id
                                    });
    output_size_[0] = 1; // return c_id only
    p_types_[0] = TPCC_ORDER_STATUS_0;
    order_status_shard(TPCC_TB_CUSTOMER, req.input_, sharding_[0]);

    status_[0] = READY;  // piece 0 ready

    order_status_dep_.piece_last2id = false;
    order_status_dep_.piece_ori_last2id = false;

    status_[1] = WAITING; // piece 1 waiting for piece 0
    status_[2] = WAITING; // piece 2 waiting for piece 0
  }

  // piece 1, R customer, depends on piece 0 if using c_last instead of c_id
  inputs_[1] = map<int32_t, Value>({
      {0, req.input_[0]},  // 0 ==>    c_w_id
      {1, req.input_[1]},  // 1 ==>    c_d_id
      {2, req.input_[2]}   // 2 ==>    c_id, may depends on piece 0
                                   });
  output_size_[1] = 4;
  p_types_[1] = TPCC_ORDER_STATUS_1;
  order_status_shard(TPCC_TB_CUSTOMER, req.input_, sharding_[1]);

  // piece 2, R order, depends on piece 0 if using c_last instead of c_id
  inputs_[2] = map<int32_t, Value>({
                                       {0, req.input_[0]},
                                       // 0 ==>    o_w_id
                                       {1, req.input_[1]},
                                       // 1 ==>    o_d_id
                                       {2, req.input_[2]}
                                       // 2 ==>    o_c_id, may depends on piece 0
                                   });
  output_size_[2] = 3;
  p_types_[2] = TPCC_ORDER_STATUS_2;
  order_status_shard(TPCC_TB_ORDER, req.input_, sharding_[2]);

  // piece 3, R order_line, depends on piece 2
  inputs_[3] = map<int32_t, Value>({
                                       {0, req.input_[0]},
                                       // 0 ==>    ol_w_id
                                       {1, req.input_[1]},
                                       // 1 ==>    ol_d_id
                                       {2, Value()}
                                       // 2 ==>    ol_o_id, depends on piece 2
                                   });
  output_size_[3] = 15 * 5;
  p_types_[3] = TPCC_ORDER_STATUS_3;
  order_status_shard(TPCC_TB_ORDER_LINE, req.input_, sharding_[3]);
  status_[3] = WAITING;

}

bool TpccChopper::order_status_callback(int pi,
                                        int res,
                                        const Value *output,
                                        uint32_t output_size) {
  if (pi == 0) {
    verify(!order_status_dep_.piece_last2id);
    order_status_dep_.piece_last2id = true;
    inputs_[1][2] = output[0];
    status_[1] = READY;
    inputs_[2][2] = output[0];
    status_[2] = READY;

    return true;
  }
  else if (pi == 1) {
    return false;
  }
  else if (pi == 2) {
    verify(output_size == 3);
    verify(!order_status_dep_.piece_order);
    order_status_dep_.piece_order = true;
    inputs_[3][2] = output[0];
    status_[3] = READY;
    return true;
  }
  else if (pi == 3) {
    return false;
  }
  else
    verify(0);
}

void TpccChopper::order_status_retry() {
  order_status_dep_.piece_last2id = order_status_dep_.piece_ori_last2id;
  order_status_dep_.piece_order = false;
  if (order_status_dep_.piece_last2id) {
    status_[0] = FINISHED;
    status_[1] = READY;
    status_[2] = READY;
    n_pieces_out_ = 1;
  }
  else {
    status_[0] = READY;
    status_[1] = WAITING;
    status_[2] = WAITING;
  }
  status_[3] = WAITING;
}

void TpccChopper::delivery_shard(const char *tb,
                                 const std::vector<mdb::Value> &input,
                                 uint32_t &site,
                                 int cnt) {
  MultiValue mv;
  if (tb == TPCC_TB_NEW_ORDER
      || tb == TPCC_TB_ORDER
      || tb == TPCC_TB_ORDER_LINE
      || tb == TPCC_TB_CUSTOMER)
    // based on w_id
    mv = MultiValue(input[0]);
  else
    verify(0);
  int ret = sss_->get_site_id_from_tb(tb, mv, site);
  verify(ret == 0);
}

//void TpccChopper::delivery_init(TxnRequest &req) {
//    /**
//     * req.input_
//     *  0       ==> w_id
//     *  1       ==> o_carrier_id
//     *  2       ==> number of district
//     **/
//    int d_cnt = req.input_[2].get_i32();
//    n_pieces_ = d_cnt * 4;
//    inputs_.resize(n_pieces_);
//    output_size_.resize(n_pieces_);
//    p_types_.resize(n_pieces_);
//    sharding_.resize(n_pieces_);
//    status_.resize(n_pieces_);
//
//    delivery_dep_.d_cnt = (size_t)d_cnt;
//    delivery_dep_.piece_new_orders = (bool *)malloc(sizeof(bool) * d_cnt);
//    delivery_dep_.piece_orders = (bool *)malloc(sizeof(bool) * d_cnt);
//    delivery_dep_.piece_order_lines = (bool *)malloc(sizeof(bool) * d_cnt);
//    memset(delivery_dep_.piece_new_orders, false, sizeof(bool) * d_cnt);
//    memset(delivery_dep_.piece_orders, false, sizeof(bool) * d_cnt);
//    memset(delivery_dep_.piece_order_lines, false, sizeof(bool) * d_cnt);
//
//    for (int i = 0; i < d_cnt; i++) {
//        // piece 4 * i, Ri & W new_order
//        inputs_[TPCC_DELIVERY_Ith_INDEX_NEW_ORDER(i)] = std::vector<Value>({
//                req.input_[0],  // 0 ==>    no_w_id
//                Value((i32)i)   // 1 ==>    no_d_id
//                });
//        output_size_[TPCC_DELIVERY_Ith_INDEX_NEW_ORDER(i)] = 1;
//        p_types_[TPCC_DELIVERY_Ith_INDEX_NEW_ORDER(i)] = TPCC_DELIVERY_0;
//        delivery_shard(TPCC_TB_NEW_ORDER, req.input_, sharding_[TPCC_DELIVERY_Ith_INDEX_NEW_ORDER(i)], i);
//        status_[TPCC_DELIVERY_Ith_INDEX_NEW_ORDER(i)] = 0;
//
//        // piece 4 * i + 1, Ri & W order
//        inputs_[TPCC_DELIVERY_Ith_INDEX_ORDER(i)] = std::vector<Value>({
//                Value(),        // 0 ==>    o_id,   depends on piece 4 * i
//                req.input_[0],  // 1 ==>    o_w_id
//                Value((i32)i),  // 2 ==>    o_d_id
//                req.input_[1]   // 3 ==>    o_carrier_id
//                });
//        output_size_[TPCC_DELIVERY_Ith_INDEX_ORDER(i)] = 1;
//        p_types_[TPCC_DELIVERY_Ith_INDEX_ORDER(i)] = TPCC_DELIVERY_1;
//        delivery_shard(TPCC_TB_ORDER, req.input_, sharding_[TPCC_DELIVERY_Ith_INDEX_ORDER(i)], i);
//        status_[TPCC_DELIVERY_Ith_INDEX_ORDER(i)] = -1;
//
//        // piece 4 * i + 2, Ri & W order_line
//        inputs_[TPCC_DELIVERY_Ith_INDEX_ORDER_LINE(i)] = std::vector<Value>({
//                Value(),        // 0 ==>    ol_o_id,   depends on piece 4 * i
//                req.input_[0],  // 1 ==>    ol_w_id
//                Value((i32)i)   // 2 ==>    ol_d_id
//                });
//        output_size_[TPCC_DELIVERY_Ith_INDEX_ORDER_LINE(i)] = 1;
//        p_types_[TPCC_DELIVERY_Ith_INDEX_ORDER_LINE(i)] = TPCC_DELIVERY_2;
//        delivery_shard(TPCC_TB_ORDER_LINE, req.input_, sharding_[TPCC_DELIVERY_Ith_INDEX_ORDER_LINE(i)], i);
//        status_[TPCC_DELIVERY_Ith_INDEX_ORDER_LINE(i)] = -1;
//
//        // piece 4 * i + 3, W customer
//        inputs_[TPCC_DELIVERY_Ith_INDEX_CUSTOMER(i)] = std::vector<Value>({
//                Value(),        // 0 ==>    c_id,   depends on piece 4 * i + 1
//                req.input_[0],  // 1 ==>    c_w_id
//                Value((i32)i),  // 2 ==>    c_d_id
//                Value()         // 3 ==>    ol_amount, depends on piece 4 * i + 2
//                });
//        output_size_[TPCC_DELIVERY_Ith_INDEX_CUSTOMER(i)] = 0;
//        p_types_[TPCC_DELIVERY_Ith_INDEX_CUSTOMER(i)] = TPCC_DELIVERY_3;
//        delivery_shard(TPCC_TB_CUSTOMER, req.input_, sharding_[TPCC_DELIVERY_Ith_INDEX_CUSTOMER(i)], i);
//        status_[TPCC_DELIVERY_Ith_INDEX_CUSTOMER(i)] = -1;
//
//    }
//}

void TpccChopper::delivery_init(TxnRequest &req) {
  /**
   * req.input_
   *  0       ==> w_id
   *  1       ==> o_carrier_id
   *  2       ==> d_id
   **/
  int d_id = req.input_[2].get_i32();
  n_pieces_all_ = 4;
//  inputs_.resize(n_pieces_all_);
  output_size_.resize(n_pieces_all_);
  p_types_.resize(n_pieces_all_);
  sharding_.resize(n_pieces_all_);
  status_.resize(n_pieces_all_);

  delivery_dep_.piece_new_orders = false;
  delivery_dep_.piece_orders = false;
  delivery_dep_.piece_order_lines = false;
  //delivery_dep_.d_cnt = (size_t)d_cnt;
  //delivery_dep_.piece_new_orders = (bool *)malloc(sizeof(bool) * d_cnt);
  //delivery_dep_.piece_orders = (bool *)malloc(sizeof(bool) * d_cnt);
  //delivery_dep_.piece_order_lines = (bool *)malloc(sizeof(bool) * d_cnt);
  //memset(delivery_dep_.piece_new_orders, false, sizeof(bool) * d_cnt);
  //memset(delivery_dep_.piece_orders, false, sizeof(bool) * d_cnt);
  //memset(delivery_dep_.piece_order_lines, false, sizeof(bool) * d_cnt);

  // piece 0, Ri & W new_order
  inputs_[0] = map<int32_t, Value>({
                                       {0, req.input_[0]},  // 0 ==>    no_w_id
                                       {1, req.input_[2]}  // 1 ==>    no_d_id
                                   });
  output_size_[0] = 1;
  p_types_[0] = TPCC_DELIVERY_0;
  delivery_shard(TPCC_TB_NEW_ORDER, req.input_, sharding_[0], d_id);
  status_[0] = READY;

  // piece 1, Ri & W order
  inputs_[1] = map<int32_t, Value>({
      {0, Value()},        // 0 ==>    o_id,   depends on piece 0
      {1, req.input_[0]},  // 1 ==>    o_w_id
      {2, req.input_[2]},  // 2 ==>    o_d_id
      {3, req.input_[1]}   // 3 ==>    o_carrier_id
                                   });
  output_size_[1] = 1;
  p_types_[1] = TPCC_DELIVERY_1;
  delivery_shard(TPCC_TB_ORDER, req.input_, sharding_[1], d_id);
  status_[1] = WAITING;

  // piece 2, Ri & W order_line
  inputs_[2] = map<int32_t, Value>({
      {0, Value()},        // 0 ==>    ol_o_id,   depends on piece 0
      {1, req.input_[0]},  // 1 ==>    ol_w_id
      {2, req.input_[2]}   // 2 ==>    ol_d_id
                                   });
  output_size_[2] = 1;
  p_types_[2] = TPCC_DELIVERY_2;
  delivery_shard(TPCC_TB_ORDER_LINE, req.input_, sharding_[2], d_id);
  status_[2] = WAITING;

  // piece 3, W customer
  inputs_[3] = map<int32_t, Value>({
      {0, Value()},        // 0 ==>    c_id,   depends on piece 1
      {1, req.input_[0]},  // 1 ==>    c_w_id
      {2, req.input_[2]},  // 2 ==>    c_d_id
      {3, Value()}         // 3 ==>    ol_amount, depends on piece 2
                                   });
  output_size_[3] = 0;
  p_types_[3] = TPCC_DELIVERY_3;
  delivery_shard(TPCC_TB_CUSTOMER, req.input_, sharding_[3], d_id);
  status_[3] = WAITING;
}

//bool TpccChopper::delivery_callback(int pi, int res, const Value *output, uint32_t output_size) {
//    if (TPCC_DELIVERY_IS_NEW_ORDER_INDEX(pi)) {
//        verify(output_size == 1);
//        verify(!delivery_dep_.piece_new_orders[TPCC_DELIVERY_INDEX_NEW_ORDER_TO_CNT(pi)]);
//        if (output[0].get_i32() == (i32)-1) { // new_order not found
//            status_[TPCC_DELIVERY_INDEX_NEW_ORDER_TO_ORDER(pi)] = 2;
//            status_[TPCC_DELIVERY_INDEX_NEW_ORDER_TO_ORDER_LINE(pi)] = 2;
//            status_[TPCC_DELIVERY_INDEX_NEW_ORDER_TO_CUSTOMER(pi)] = 2;
//            n_started_ += 3;
//            Log::info("TPCC DELIVERY: no more new order for w_id: %d, d_id: %d", inputs_[pi][0].get_i32(), inputs_[pi][1].get_i32());
//            return false;
//        }
//        else {
//            inputs_[TPCC_DELIVERY_INDEX_NEW_ORDER_TO_ORDER(pi)][0] = output[0];
//            inputs_[TPCC_DELIVERY_INDEX_NEW_ORDER_TO_ORDER_LINE(pi)][0] = output[0];
//            status_[TPCC_DELIVERY_INDEX_NEW_ORDER_TO_ORDER(pi)] = 0;
//            status_[TPCC_DELIVERY_INDEX_NEW_ORDER_TO_ORDER_LINE(pi)] = 0;
//            delivery_dep_.piece_new_orders[TPCC_DELIVERY_INDEX_NEW_ORDER_TO_CNT(pi)] = true;
//            return true;
//        }
//    }
//    else if (TPCC_DELIVERY_IS_ORDER_INDEX(pi)) {
//        verify(output_size == 1);
//        inputs_[TPCC_DELIVERY_INDEX_ORDER_TO_CUSTOMER(pi)][0] = output[0];
//        delivery_dep_.piece_orders[TPCC_DELIVERY_INDEX_ORDER_TO_CNT(pi)] = true;
//        if (delivery_dep_.piece_order_lines[TPCC_DELIVERY_INDEX_ORDER_TO_CNT(pi)] == true) {
//            status_[TPCC_DELIVERY_INDEX_ORDER_TO_CUSTOMER(pi)] = 0;
//            return true;
//        }
//        else
//            return false;
//    }
//    else if (TPCC_DELIVERY_IS_ORDER_LINE_INDEX(pi)) {
//        verify(output_size == 1);
//        inputs_[TPCC_DELIVERY_INDEX_ORDER_LINE_TO_CUSTOMER(pi)][3] = output[0];
//        delivery_dep_.piece_order_lines[TPCC_DELIVERY_INDEX_ORDER_LINE_TO_CNT(pi)] = true;
//        if (delivery_dep_.piece_orders[TPCC_DELIVERY_INDEX_ORDER_LINE_TO_CNT(pi)] == true) {
//            status_[TPCC_DELIVERY_INDEX_ORDER_LINE_TO_CUSTOMER(pi)] = 0;
//            return true;
//        }
//        else
//            return false;
//    }
//    else if (TPCC_DELIVERY_IS_CUSTOMER(pi)) {
//        return false;
//    }
//    else
//        verify(0);
//}

bool TpccChopper::delivery_callback(int pi,
                                    int res,
                                    const Value *output,
                                    uint32_t output_size) {
//  if (pi == 0) {
//    verify(output_size == 1);
//    verify(!delivery_dep_.piece_new_orders);
//    if (output[0].get_i32() == (i32) -1) { // new_order not found
//      status_[1] = FINISHED;
//      status_[2] = FINISHED;
//      status_[3] = FINISHED;
//      n_pieces_out_ += 3;
//      Log::info("TPCC DELIVERY: no more new order for w_id: %d, d_id: %d",
//                inputs_[pi][0].get_i32(),
//                inputs_[pi][1].get_i32());
//      return false;
//    }
//    else {
//      inputs_[1][0] = output[0];
//      inputs_[2][0] = output[0];
//      status_[1] = READY;
//      status_[2] = READY;
//      delivery_dep_.piece_new_orders = true;
//      return true;
//    }
//  }
//  else if (pi == 1) {
//    verify(output_size == 1);
//    inputs_[3][0] = output[0];
//    delivery_dep_.piece_orders = true;
//    if (delivery_dep_.piece_order_lines) {
//      status_[3] = READY;
//      return true;
//    }
//    else
//      return false;
//  }
//  else if (pi == 2) {
//    verify(output_size == 1);
//    inputs_[3][3] = output[0];
//    delivery_dep_.piece_order_lines = true;
//    if (delivery_dep_.piece_orders) {
//      status_[3] = READY;
//      return true;
//    }
//    else
//      return false;
//  }
//  else if (pi == 3) {
//    return false;
//  }
//  else
    verify(0);
}

//void TpccChopper::delivery_retry() {
//    for (size_t i = 0; i < delivery_dep_.d_cnt; i++) {
//        status_[TPCC_DELIVERY_Ith_INDEX_NEW_ORDER(i)] = 0;
//        status_[TPCC_DELIVERY_Ith_INDEX_ORDER(i)] = -1;
//        status_[TPCC_DELIVERY_Ith_INDEX_ORDER_LINE(i)] = -1;
//        status_[TPCC_DELIVERY_Ith_INDEX_CUSTOMER(i)] = -1;
//        delivery_dep_.piece_new_orders[i] = false;
//        delivery_dep_.piece_orders[i] = false;
//        delivery_dep_.piece_order_lines[i] = false;
//    }
//}

void TpccChopper::delivery_retry() {
  status_[0] = READY;
  status_[1] = WAITING;
  status_[2] = WAITING;
  status_[3] = WAITING;
  delivery_dep_.piece_new_orders = false;
  delivery_dep_.piece_orders = false;
  delivery_dep_.piece_order_lines = false;
}

void TpccChopper::stock_level_shard(
    const char *tb,
    const std::vector<mdb::Value> &input,
    uint32_t &site) {
  MultiValue mv;
  if (tb == TPCC_TB_DISTRICT
      || tb == TPCC_TB_ORDER_LINE)
    // based on w_id
    mv = MultiValue(input[0]);
  else if (tb == TPCC_TB_STOCK)
    // based on s_w_id
    mv = MultiValue(input[1]);
  else
    verify(0);
  int ret = sss_->get_site_id_from_tb(tb, mv, site);
  verify(ret == 0);
}

void TpccChopper::stock_level_shard(
    const char *tb,
    const map<int32_t, Value> &input,
    uint32_t &site) {
  MultiValue mv;
  if (tb == TPCC_TB_DISTRICT
      || tb == TPCC_TB_ORDER_LINE)
    // based on w_id
    mv = MultiValue(input.at(0));
  else if (tb == TPCC_TB_STOCK)
    // based on s_w_id
    mv = MultiValue(input.at(1));
  else
    verify(0);
  int ret = sss_->get_site_id_from_tb(tb, mv, site);
  verify(ret == 0);
}

void TpccChopper::stock_level_init(TxnRequest &req) {
  /**
   * req.input_
   *  0       ==> w_id
   *  1       ==> d_id
   *  2       ==> threshold
   **/
  n_pieces_all_ = 2;
//  inputs_.resize(n_pieces_all_);
  output_size_.resize(n_pieces_all_);
  p_types_.resize(n_pieces_all_);
  sharding_.resize(n_pieces_all_);
  status_.resize(n_pieces_all_);

  stock_level_dep_.w_id = req.input_[0].get_i32();
  stock_level_dep_.threshold = req.input_[2].get_i32();

  // piece 0, R district
  inputs_[0] = map<int32_t, Value>({
      {0, req.input_[0]},  // 0    ==> w_id
      {1, req.input_[1]}   // 1    ==> d_id
                                  });
  output_size_[0] = 1;
  p_types_[0] = TPCC_STOCK_LEVEL_0;
  stock_level_shard(TPCC_TB_DISTRICT, req.input_, sharding_[0]);
  status_[0] = READY;

  // piece 1, R order_line
  inputs_[1] = map<int32_t, Value>({
      {0, Value()},        // 0    ==> d_next_o_id, depends on piece 0
      {1, req.input_[0]},  // 1    ==> ol_w_id
      {2, req.input_[1]}   // 2    ==> ol_d_id
                                  });
  output_size_[1] = 20 * 15; // 20 orders * 15 order_line per order at most
  p_types_[1] = TPCC_STOCK_LEVEL_1;
  stock_level_shard(TPCC_TB_ORDER_LINE, req.input_, sharding_[1]);
  status_[1] = WAITING;

  // piece 2 - n, R stock init in stock_level_callback
}

bool TpccChopper::stock_level_callback(
    int pi,
    int res,
    const Value *output,
    uint32_t output_size) {
  verify(0);
  if (pi == 0) {

  } else if (pi == 1) {

  }
  else
    return false;
}

void TpccChopper::stock_level_retry() {
  n_pieces_all_ = 2;
//  inputs_.resize(n_pieces_all_);
  output_size_.resize(n_pieces_all_);
  p_types_.resize(n_pieces_all_);
  sharding_.resize(n_pieces_all_);
  status_.resize(n_pieces_all_);
  status_[0] = READY;
  status_[1] = WAITING;
}

void TpccChopper::retry() {
  partitions_.clear();
  n_pieces_out_ = 0;
  n_try_++;
  commit_.store(true);
  switch (txn_type_) {
    case TPCC_NEW_ORDER:
      new_order_retry();
      break;
    case TPCC_PAYMENT:
      payment_retry();
      break;
    case TPCC_ORDER_STATUS:
      order_status_retry();
      break;
    case TPCC_DELIVERY:
      delivery_retry();
      break;
    case TPCC_STOCK_LEVEL:
      stock_level_retry();
      break;
    default:
      verify(0);
  }
}

bool TpccChopper::is_read_only() {
  switch (txn_type_) {
    case TPCC_NEW_ORDER:
      return false;
    case TPCC_PAYMENT:
      return false;
    case TPCC_ORDER_STATUS:
      return true;
    case TPCC_DELIVERY:
      return false;
    case TPCC_STOCK_LEVEL:
      return true;
    default:
      verify(0);
  }
}

TpccChopper::~TpccChopper() {
  if (txn_type_ == TPCC_NEW_ORDER) {
    free(new_order_dep_.piece_items);
    free(new_order_dep_.piece_stocks);
  }
  //else if (txn_type_ == TPCC_DELIVERY) {
  //    free(delivery_dep_.piece_new_orders);
  //    free(delivery_dep_.piece_orders);
  //    free(delivery_dep_.piece_order_lines);
  //}
}

} // namespace rococo
