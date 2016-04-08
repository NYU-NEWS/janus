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

TpccTxn::TpccTxn() {
}

void TpccTxn::Init(TxnRequest &req) {
  ws_init_ = req.input_;
  ws_ = ws_init_;
  type_ = req.txn_type_;
  callback_ = req.callback_;
  max_try_ = req.n_try_;
  n_try_ = 1;
  commit_.store(true);

  switch (type_) {
    case TPCC_NEW_ORDER:
      NewOrderInit(req);
      break;
    case TPCC_PAYMENT:
      PaymentInit(req);
      break;
    case TPCC_ORDER_STATUS:
      OrderStatusInit(req);
      break;
    case TPCC_DELIVERY:
      DeliveryInit(req);
      break;
    case TPCC_STOCK_LEVEL:
      StockLevelInit(req);
      break;
    default:
      verify(0);
  }
  verify(n_pieces_input_ready_ > 0);
}

// This is sort of silly. We should have a better way.
bool TpccTxn::CheckReady() {
  bool ret = false;
  auto &map = txn_reg_->input_vars_[type_];
  for (auto &kv : map) {
    auto pi = kv.first;
    auto &var_set = kv.second;

    if (status_.find(pi) == status_.end()) {
      continue;
    }
    bool all_found = true;
    for (auto &var : var_set) {
      if (ws_.find(var) == ws_.end()) {
        // not found. input not all ready.
        all_found = false;
        break;
      } else {
        inputs_[pi][var] = ws_[var];
        verify(ws_[var].get_kind() != 0);
      }
    }
    // all found.
    if (all_found && status_[pi] == WAITING) {
      status_[pi] = READY;
      n_pieces_input_ready_++;
      ret = true;
//        for (auto &var : var_set) {
//          inputs_[pi][var] = ws_[var];
//          verify(ws_[var].get_kind() != 0);
//        }
    }
  }
  return ret;
}

bool TpccTxn::start_callback(int pi,
                                 int res,
                                 map<int32_t, Value> &output_map) {
  bool ret;
  ws_.insert(output_map.begin(), output_map.end());
  if (type_ == TPCC_PAYMENT ||
      type_ == TPCC_ORDER_STATUS ||
      type_ == TPCC_DELIVERY ||
      type_ == TPCC_NEW_ORDER ||
      0) {
    return CheckReady();
  }
  PieceCallbackHandler handler;
  auto it = txn_reg_->callbacks_.find(std::make_pair(type_, pi));
  if (it != txn_reg_->callbacks_.end()) {
    handler = it->second;
    ret = handler(this, output_map);

  } else {
//    ws_.insert(output_map.begin(), output_map.end());
    bool ret = CheckReady();
//
//    handler = [] (TxnChopper* ch,
//                  map<int32_t, Value>& output) -> bool { return false; };
  }

  // below is for debug
  if (type_ == TPCC_STOCK_LEVEL && pi == TPCC_STOCK_LEVEL_0) {
    verify(ws_.count(TPCC_VAR_D_NEXT_O_ID) > 0);
    verify(inputs_[TPCC_STOCK_LEVEL_1].count(TPCC_VAR_D_NEXT_O_ID) > 0);
    verify(status_[TPCC_STOCK_LEVEL_1] == READY);
  }
//  if (txn_type_ == TPCC_NEW_ORDER && pi == TPCC_NEW_ORDER_0) {
//    verify(ws_.find(TPCC_VAR_O_ID) != ws_.end());
//    verify(inputs_[TPCC_NEW_ORDER_3].count(TPCC_VAR_O_ID) > 0);
//    verify(inputs_[TPCC_NEW_ORDER_4].count(TPCC_VAR_O_ID) > 0);
//
//
//    auto &vars = txn_reg_->input_vars_[TPCC_NEW_ORDER][TPCC_NEW_ORDER_3];
//    for (auto v : vars) {
//      verify(ws_.count(v) > 0);
//    }
//
//    verify(status_[TPCC_NEW_ORDER_3] == READY);
//    verify(status_[TPCC_NEW_ORDER_4] == READY);
//
//    for (size_t i = 0; i < new_order_dep_.ol_cnt; i++) {
//      auto pi = TPCC_NEW_ORDER_Ith_INDEX_ORDER_LINE(i);
//      verify(inputs_[pi].count(TPCC_VAR_O_ID) > 0);
//      bool b2 = (ws_.find(TPCC_VAR_I_PRICE(i)) != ws_.end());
//      bool b3 = (ws_.find(TPCC_VAR_OL_DIST_INFO(i)) != ws_.end());
////      verify(b2 == tpcc_ch->new_order_dep_.piece_items[i]);
////      verify(b3 == tpcc_ch->new_order_dep_.piece_stocks[i]);
//      if (b2 && b3)
//        verify(status_[pi] == READY);
//    }
//  }
  return ret;


  return handler(this, output_map);
}

void TpccTxn::Reset() {
  TxnCommand::Reset();
  ws_ = ws_init_;
  partition_ids_.clear();
  n_try_++;
  commit_.store(true);
  n_pieces_input_ready_ = 0;
  n_pieces_replied_ = 0;
  n_pieces_out_ = 0;
  switch (type_) {
    case TPCC_NEW_ORDER:
      NewOrderRetry();
      break;
    case TPCC_PAYMENT:
      PaymentRetry();
      break;
    case TPCC_ORDER_STATUS:
      OrderStatusRetry();
      break;
    case TPCC_DELIVERY:
      DeliveryRetry();
      break;
    case TPCC_STOCK_LEVEL:
      StockLevelRetry();
      break;
    default:
      verify(0);
  }
  verify(n_pieces_input_ready_ > 0);
}

bool TpccTxn::IsReadOnly() {
  switch (type_) {
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

parid_t TpccTxn::GetPiecePartitionId(innid_t inn_id) {
  parid_t partition_id;
  auto it = txn_reg_->sharding_input_.find(std::make_pair(type_, inn_id));
  if (it != txn_reg_->sharding_input_.end()) {
    auto &pair = it->second;
    auto tb = pair.first;
    auto& var_ids = pair.second;
//    auto cmd = (SimpleCommand*) inputs_[inn_id];
//    verify(inputs_.find(inn_id) != inputs_.end());
    vector<Value> vars;
    for (auto var_id : var_ids) {
      verify(ws_.find(var_id) != ws_.end());
      vars.push_back(ws_.at(var_id));
    }
    MultiValue mv = MultiValue(vars);
    sss_->GetPartition(tb, mv, partition_id);
  } else {
    verify(0);
    partition_id = sharding_[inn_id];
  }
  return partition_id;
}

int TpccTxn::GetNPieceAll() {
  if (type_ == TPCC_STOCK_LEVEL) {
    verify(ws_.count(TPCC_VAR_OL_AMOUNT) > 0 == ws_.count(TPCC_VAR_N_PIECE_ALL) > 0);
    if (ws_.count(TPCC_VAR_OL_AMOUNT) > 0) {
      verify(ws_[TPCC_VAR_N_PIECE_ALL].get_i32() == n_pieces_all_);
      return n_pieces_all_;
      return n_pieces_all_ + ws_[TPCC_VAR_OL_AMOUNT].get_i32();
    } else {
      return n_pieces_all_;
    }
  }
  return n_pieces_all_;
}

TpccTxn::~TpccTxn() {
  //else if (txn_type_ == TPCC_DELIVERY) {
  //    free(delivery_dep_.piece_new_orders);
  //    free(delivery_dep_.piece_orders);
  //    free(delivery_dep_.piece_order_lines);
  //}
}

} // namespace rococo
