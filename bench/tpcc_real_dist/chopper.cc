#include "all.h"

namespace deptran {

TpccRealDistChopper::TpccRealDistChopper() {
}

siteid_t TpccRealDistChopper::GetPiecePar(innid_t inn_id) {
  siteid_t site_id;
  if (type_ == TPCC_NEW_ORDER ||
      type_ == TPCC_PAYMENT ||
      type_ == TPCC_DELIVERY ||
      type_ == TPCC_ORDER_STATUS ||
      type_ == TPCC_STOCK_LEVEL) {
  } else {
    verify(0);
  }
  site_id = TpccChopper::GetPieceSiteId(inn_id);
  return site_id;
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

}
