#include "deptran/__dep__.h"
#include "chopper.h"
#include "piece.h"

namespace rococo {

TpccRealDistChopper::TpccRealDistChopper() {
}

siteid_t TpccRealDistChopper::GetPiecePar(innid_t inn_id) {
  parid_t partition_id;
  if (type_ == TPCC_NEW_ORDER ||
      type_ == TPCC_PAYMENT ||
      type_ == TPCC_DELIVERY ||
      type_ == TPCC_ORDER_STATUS ||
      type_ == TPCC_STOCK_LEVEL) {
  } else {
    verify(0);
  }
  partition_id = TpccTxn::GetPiecePartitionId(inn_id);
  return partition_id;
}

bool TpccRealDistChopper::IsOneRound() {
  switch (type_) {
    case TPCC_NEW_ORDER:
    case TPCC_PAYMENT:
    case TPCC_DELIVERY:
    case TPCC_ORDER_STATUS:
    case TPCC_STOCK_LEVEL:
      return false;
  }
}

TpccRealDistChopper::~TpccRealDistChopper() {
}

} // namespace rococo
