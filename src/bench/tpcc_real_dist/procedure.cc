#include "deptran/__dep__.h"
#include "procedure.h"
#include "workload.h"

namespace janus {

TpccRdProcedure::TpccRdProcedure() {
}

siteid_t TpccRdProcedure::GetPiecePar(innid_t inn_id) {
  parid_t partition_id;
  if (type_ == TPCC_NEW_ORDER ||
      type_ == TPCC_PAYMENT ||
      type_ == TPCC_DELIVERY ||
      type_ == TPCC_ORDER_STATUS ||
      type_ == TPCC_STOCK_LEVEL) {
  } else {
    verify(0);
  }
  partition_id = TpccProcedure::GetPiecePartitionId(inn_id);
  return partition_id;
}

bool TpccRdProcedure::IsOneRound() {
  switch (type_) {
    case TPCC_NEW_ORDER:
    case TPCC_PAYMENT:
    case TPCC_DELIVERY:
    case TPCC_ORDER_STATUS:
    case TPCC_STOCK_LEVEL:
      return false;
  }
  return false;
}

TpccRdProcedure::~TpccRdProcedure() {
}

} // namespace janus
