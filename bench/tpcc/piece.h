#ifndef TPCC_PIECE_H_
#define TPCC_PIECE_H_

#include "deptran/piece.h"

namespace rococo {


// ======= New order txn =======
#define TPCC_NEW_ORDER              10
#define TPCC_NEW_ORDER_NAME         "NEW ORDER"
#define TPCC_NEW_ORDER_0            100
#define TPCC_NEW_ORDER_1            101
#define TPCC_NEW_ORDER_2            102
#define TPCC_NEW_ORDER_3            103
#define TPCC_NEW_ORDER_4            104
#define TPCC_NEW_ORDER_5            105
#define TPCC_NEW_ORDER_6            106
#define TPCC_NEW_ORDER_7            107
#define TPCC_NEW_ORDER_8            108

// ======== Payment txn ========
#define TPCC_PAYMENT                20
#define TPCC_PAYMENT_NAME           "PAYMENT"
#define TPCC_PAYMENT_0              200
#define TPCC_PAYMENT_1              201
#define TPCC_PAYMENT_2              202
#define TPCC_PAYMENT_3              203
#define TPCC_PAYMENT_4              204
#define TPCC_PAYMENT_5              205

// ===== Order status txn ======
#define TPCC_ORDER_STATUS           30
#define TPCC_ORDER_STATUS_NAME      "ORDER STATUS"
#define TPCC_ORDER_STATUS_0         300
#define TPCC_ORDER_STATUS_1         301
#define TPCC_ORDER_STATUS_2         302
#define TPCC_ORDER_STATUS_3         303

// ======= Delivery txn ========
#define TPCC_DELIVERY               40
#define TPCC_DELIVERY_NAME          "DELIVERY"
#define TPCC_DELIVERY_0             400
#define TPCC_DELIVERY_1             401
#define TPCC_DELIVERY_2             402
#define TPCC_DELIVERY_3             403

// ====== Stock level txn ======
#define TPCC_STOCK_LEVEL            50
#define TPCC_STOCK_LEVEL_NAME       "STOCK LEVEL"
#define TPCC_STOCK_LEVEL_0          500
#define TPCC_STOCK_LEVEL_1          501
#define TPCC_STOCK_LEVEL_2          502

// context id
#define ROW_DISTRICT          __LINE__
#define ROW_DISTRICT_TEMP     __LINE__
#define ROW_WAREHOUSE         __LINE__
#define ROW_CUSTOMER          __LINE__
#define ROW_ORDER             __LINE__
#define ROW_ORDER_SEC         (header.pid)
#define ROW_ITEM              (header.pid)
#define ROW_STOCK             (header.pid)
#define ROW_STOCK_TEMP        (header.pid)

extern char TPCC_TB_WAREHOUSE[];
extern char TPCC_TB_DISTRICT[];
extern char TPCC_TB_CUSTOMER[];
extern char TPCC_TB_HISTORY[];
extern char TPCC_TB_ORDER[];
extern char TPCC_TB_NEW_ORDER[];
extern char TPCC_TB_ITEM[];
extern char TPCC_TB_STOCK[];
extern char TPCC_TB_ORDER_LINE[];
extern char TPCC_TB_ORDER_C_ID_SECONDARY[];

class TpccPiece: public Piece {
 protected:
  // new order
  virtual void reg_new_order();

  // payment
  virtual void reg_payment();

  //void reg_payment_remote();
  //void reg_payment_remote_case1();
  //void reg_payment_remote_case2();

  //void reg_payment_home();
  //void reg_payment_home_case1();
  //void reg_payment_home_case2();

  // order status
  virtual void reg_order_status();

  // delivery
  virtual void reg_delivery();

  // stock level
  virtual void reg_stock_level();

 public:
  virtual void reg_all();

  TpccPiece();

  virtual ~TpccPiece();

};

}

#endif
