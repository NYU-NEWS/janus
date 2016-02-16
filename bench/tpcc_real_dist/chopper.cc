#include "all.h"

namespace deptran {

TpccRealDistChopper::TpccRealDistChopper() {
}

parid_t TpccRealDistChopper::GetPiecePar(innid_t inn_id) {
  parid_t par_id;
  if (type_ == TPCC_NEW_ORDER ||
      type_ == TPCC_PAYMENT ||
      type_ == TPCC_DELIVERY ||
      type_ == TPCC_ORDER_STATUS ||
      type_ == TPCC_STOCK_LEVEL) {
  } else {
    verify(0);
  }
  par_id = TpccChopper::GetPiecePar(inn_id);
  return par_id;
}


TpccRealDistChopper::~TpccRealDistChopper() {
}

}
