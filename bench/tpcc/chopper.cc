#include "deptran/__dep__.h"
#include "deptran/txn_chopper.h"
#include "chopper.h"
#include "piece.h"

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
  input_vars_ = txn_reg_->input_vars_[type_];
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
  verify(n_pieces_dispatchable_ > 0);
}

// This is sort of silly. We should have a better way.
bool TpccTxn::CheckReady() {
  bool ret = false;
  map<innid_t, set<int32_t>>& map = input_vars_;
  for (auto &pair : status_) {
    const innid_t& pi = pair.first;
    int32_t& status = pair.second;
    if (status != WAITING) {
      continue;
    }
    set<int32_t>& var_set = map[pi];
    bool all_found = true;
    for (auto &var : var_set) {
      if (ws_.count(var) == 0) {
        // not found. input not all ready.
        all_found = false;
        break;
      } else {
#ifdef DEBUG_CODE
    TxnWorkspace& ws = GetWorkspace(pi);
    if (ws.keys_.size() == 0)
      ws.keys_ = var_set;
    verify(ws_[var].get_kind() != 0);
#endif
      }
    }
    // all found.
    if (all_found && status == WAITING) {
      status = DISPATCHABLE;
      TxnWorkspace& ws = GetWorkspace(pi);
      ws.keys_ = var_set;
      n_pieces_dispatchable_++;
      ret = true;
    }
  }
  return ret;
}

bool TpccTxn::start_callback(int pi,
                             int res,
                             map<int32_t, Value> &output_map) {
  bool ret;

  ws_.insert(output_map);
  if (type_ == TPCC_PAYMENT ||
      type_ == TPCC_ORDER_STATUS ||
      type_ == TPCC_DELIVERY ||
      type_ == TPCC_NEW_ORDER ||
      0) {
    // for debug

    auto ret = CheckReady();
    if (type_ == TPCC_DELIVERY) {
      if (pi == TPCC_DELIVERY_2) {
        verify(output_map.count(TPCC_VAR_OL_AMOUNT) > 0);
        verify(ws_.count(TPCC_VAR_OL_AMOUNT) > 0);
      }
      if (pi == TPCC_DELIVERY_1) {
        verify(output_map.count(TPCC_VAR_C_ID) > 0);
        verify(ws_.count(TPCC_VAR_C_ID) > 0);
      }
    }
    return ret;
  }
  PieceCallbackHandler handler;
  auto it = txn_reg_->callbacks_.find(std::make_pair(type_, pi));
  if (it != txn_reg_->callbacks_.end()) {
    handler = it->second;
    ret = handler(this, output_map);
  } else {
    // ws_.insert(output_map.begin(), output_map.end());
    bool ret = CheckReady();
  }

  // below is for debug
  if (type_ == TPCC_STOCK_LEVEL && pi == TPCC_STOCK_LEVEL_0) {
    verify(ws_.count(TPCC_VAR_D_NEXT_O_ID) > 0);
    TxnWorkspace& ws = GetWorkspace(TPCC_STOCK_LEVEL_1);
    verify(ws.count(TPCC_VAR_D_NEXT_O_ID) > 0);
    verify(status_[TPCC_STOCK_LEVEL_1] == DISPATCHABLE);
  }
  return ret;
}

void TpccTxn::Reset() {
  TxnCommand::Reset();
  ws_ = ws_init_;
  partition_ids_.clear();
  n_try_++;
  commit_.store(true);
  n_pieces_dispatchable_ = 0;
  n_pieces_dispatch_acked_ = 0;
  n_pieces_dispatched_ = 0;
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
  verify(n_pieces_dispatchable_ > 0);
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
  parid_t partition_id = 0;
  auto it = txn_reg_->sharding_input_.find(std::make_pair(type_, inn_id));
  if (it != txn_reg_->sharding_input_.end()) {
    auto &pair = it->second;
    auto tb = pair.first;
    auto& var_ids = pair.second;
    vector<Value> vars;
    for (auto var_id : var_ids) {
      verify(ws_.count(var_id) != 0);
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
}

} // namespace rococo
