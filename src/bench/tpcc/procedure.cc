#include "deptran/__dep__.h"
#include "deptran/procedure.h"
#include "procedure.h"
#include "workload.h"

namespace janus {


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

TpccProcedure::TpccProcedure() {
}

void TpccProcedure::Init(TxRequest &req) {
  ws_init_ = req.input_;
  ws_ = ws_init_;
  type_ = req.tx_type_;
  callback_ = req.callback_;
  max_try_ = req.n_try_;
  n_try_ = 1;
  commit_.store(true);
  leaf_procs_ = txn_reg_.lock()->regs_[type_];
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
// TODO check_shards_ready?
bool TpccProcedure::CheckReady() {
  bool ret = false;
  for (auto &pair : status_) {
    const innid_t& pi = pair.first;
    int32_t& status = pair.second;
    if (status != WAITING) {
      continue;
    }
    set<int32_t>& input_vars = leaf_procs_[pi].input_vars_;
    auto& sharding_vars = leaf_procs_[pi].sharder_.second;
    bool all_found = true;
    for (auto &var : sharding_vars) {
      if (ws_.count(var) == 0) {
        // not found. input not all ready.
        all_found = false;
        break;
      } else {
#ifdef DEBUG_CODE
    TxWorkspace& ws = GetWorkspace(pi);
    if (ws.keys_.size() == 0)
      ws.keys_ = var_set;
    verify(ws_[var].get_kind() != 0);
#endif
      }
    }
    // sharding variables all found.
    if (all_found && status == WAITING) {
      status = DISPATCHABLE;
      TxWorkspace& ws = GetWorkspace(pi);
      ws.keys_ = input_vars;
      n_pieces_dispatchable_++;
      ret = true;
    }
  }
  return ret;
}

bool TpccProcedure::HandleOutput(int pi,
                                 int res,
                                 map<int32_t, Value> &output_map) {
  bool ret;

  ws_.insert(output_map);

  // below is for debug
  if (type_ == TPCC_PAYMENT ||
      type_ == TPCC_ORDER_STATUS ||
      type_ == TPCC_DELIVERY ||
      type_ == TPCC_NEW_ORDER ||
      0) {
    // for debug

    ret = CheckReady();
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
  // above is for debug.

  PieceCallbackHandler handler;
  auto& callback = txn_reg_.lock()->regs_[type_][pi].callback_;
  if (callback) {
    ret = callback(this, output_map);
  } else {
    // ws_.insert(output_map.begin(), output_map.end());
    ret = CheckReady();
  }

  // for debug
  if (type_ == TPCC_STOCK_LEVEL && pi == TPCC_STOCK_LEVEL_0) {
    verify(ws_.count(TPCC_VAR_D_NEXT_O_ID) > 0);
    TxWorkspace& ws = GetWorkspace(TPCC_STOCK_LEVEL_1);
    verify(ws.count(TPCC_VAR_D_NEXT_O_ID) > 0);
    verify(status_[TPCC_STOCK_LEVEL_1] == DISPATCHABLE);
  }

  return ret;
}

void TpccProcedure::Reset() {
  TxData::Reset();
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

bool TpccProcedure::IsReadOnly() {
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

parid_t TpccProcedure::GetPiecePartitionId(innid_t inn_id) {
  parid_t partition_id = 0;
  auto& pair = txn_reg_.lock()->regs_[type_][inn_id].sharder_;
  if (true) {
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

int TpccProcedure::GetNPieceAll() {
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

TpccProcedure::~TpccProcedure() = default;

} // namespace janus
