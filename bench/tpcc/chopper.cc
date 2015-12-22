#include "all.h"

namespace rococo {


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

bool TpccChopper::start_callback(int pi,
                                 int res,
                                 map<int32_t, Value> &output_map) {
  PieceCallbackHandler handler;
  auto it = txn_reg_->callbacks_.find(std::make_pair(txn_type_, pi));
  if (it != txn_reg_->callbacks_.end()) {
    handler = it->second;
  } else {
    handler = [] (TxnChopper* ch,
                  map<int32_t, Value>& output) -> bool { return false; };
  }
  return handler(this, output_map);
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
