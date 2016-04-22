#pragma once

#include "../tpcc/piece.h"

namespace rococo {

// ======= New order txn =======
#define TPCCD_NEW_ORDER              10
#define TPCCD_NEW_ORDER_NAME         "NEW ORDER"
#define TPCCD_NEW_ORDER_0            1000
#define TPCCD_NEW_ORDER_1            1001
#define TPCCD_NEW_ORDER_2            1002
#define TPCCD_NEW_ORDER_3            1003
#define TPCCD_NEW_ORDER_4            1004
//#define TPCC_NEW_ORDER_5            105
#define TPCCD_NEW_ORDER_RI(i)           (15000+i)
#define TPCCD_NEW_ORDER_RS(i)           (16000+i)
#define TPCCD_NEW_ORDER_WS(i)           (17000+i)
#define TPCCD_NEW_ORDER_WOL(i)          (18000+i)

class TpccRealDistPiece: public TpccPiece {
 public:
  using TpccPiece::TpccPiece;
  void RegNewOrder() override;
  void RegPayment() override;
  void RegDelivery() override;
  void RegOrderStatus() override;
  void RegStockLevel() override;
  virtual ~TpccRealDistPiece() { }
};

} // namespace rococo

