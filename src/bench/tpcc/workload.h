#pragma once

#include "deptran/workload.h"
#include "sharding.h"

namespace janus {

// ======= New order txn =======
#define TPCC_NEW_ORDER              10
#define TPCC_NEW_ORDER_NAME         "NEW_ORDER"
#define TPCC_NEW_ORDER_0            1000
#define TPCC_NEW_ORDER_1            1001
#define TPCC_NEW_ORDER_2            1002
#define TPCC_NEW_ORDER_3            1003
#define TPCC_NEW_ORDER_4            1004
//#define TPCC_NEW_ORDER_5            105
#define TPCC_NEW_ORDER_RI(i)           (15000+i)
#define TPCC_NEW_ORDER_RS(i)           (16000+i)
#define TPCC_NEW_ORDER_WS(i)           (17000+i)
#define TPCC_NEW_ORDER_WOL(i)          (18000+i)

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
#define TPCC_ORDER_STATUS_NAME      "ORDER_STATUS"
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
#define TPCC_STOCK_LEVEL_NAME       "STOCK_LEVEL"
#define TPCC_STOCK_LEVEL_0          500
#define TPCC_STOCK_LEVEL_1          501
#define TPCC_STOCK_LEVEL_RS(i)          (52000+i)

// context id
#define ROW_DISTRICT          __LINE__
#define ROW_DISTRICT_TEMP     __LINE__
#define ROW_WAREHOUSE         __LINE__
#define ROW_CUSTOMER          __LINE__
#define ROW_ORDER             __LINE__
#define RS_NEW_ORDER          __LINE__
#define RS_ORDER_LINE         __LINE__
#define ROW_ORDER_SEC         (cmd.id_)
#define ROW_ITEM              (cmd.id_)
#define ROW_STOCK             (cmd.id_)
#define ROW_STOCK_TEMP        (cmd.id_)

#define TPCC_COL_WAREHOUSE_W_NAME              (1)
#define TPCC_COL_WAREHOUSE_W_STREET_1          (2)
#define TPCC_COL_WAREHOUSE_W_STREET_2          (3)
#define TPCC_COL_WAREHOUSE_W_CITY              (4)
#define TPCC_COL_WAREHOUSE_W_STATE             (5)
#define TPCC_COL_WAREHOUSE_W_ZIP               (6)
#define TPCC_COL_WAREHOUSE_W_TAX               (7)
  
#define TPCC_COL_NEW_ORDER_NO_O_ID             (0)
#define TPCC_COL_NEW_ORDER_NO_D_ID             (1)
#define TPCC_COL_NEW_ORDER_NO_W_ID             (2)


#define TPCC_COL_DISTRICT_D_NAME               (2)
#define TPCC_COL_DISTRICT_D_STREET_1           (3)
#define TPCC_COL_DISTRICT_D_STREET_2           (4)
#define TPCC_COL_DISTRICT_D_CITY               (5)
#define TPCC_COL_DISTRICT_D_STATE              (6)
#define TPCC_COL_DISTRICT_D_ZIP                (7)
#define TPCC_COL_DISTRICT_D_TAX                (8)
#define TPCC_COL_DISTRICT_D_YTD                (9)
#define TPCC_COL_DISTRICT_D_NEXT_O_ID          (10)
  
#define TPCC_COL_CUSTOMER_C_FIRST              (3)  /* c_first      */ 
#define TPCC_COL_CUSTOMER_C_MIDDLE             (4)  /* c_middle     */ 
#define TPCC_COL_CUSTOMER_C_LAST               (5)  /* c_last       */ 
#define TPCC_COL_CUSTOMER_C_STREET_1           (6)  /* c_street_1   */ 
#define TPCC_COL_CUSTOMER_C_STREET_2           (7)  /* c_street_2   */ 
#define TPCC_COL_CUSTOMER_C_CITY               (8)  /* c_city       */ 
#define TPCC_COL_CUSTOMER_C_STATE              (9)  /* c_state      */ 
#define TPCC_COL_CUSTOMER_C_ZIP                (10) /* c_zip        */ 
#define TPCC_COL_CUSTOMER_C_PHONE              (11) /* c_phone      */ 
#define TPCC_COL_CUSTOMER_C_SINCE              (12) /* c_since      */ 
#define TPCC_COL_CUSTOMER_C_CREDIT             (13) /* c_credit     */ 
#define TPCC_COL_CUSTOMER_C_CREDIT_LIM         (14) /* c_credit_lim */ 
#define TPCC_COL_CUSTOMER_C_DISCOUNT           (15) /* c_discount   */ 
#define TPCC_COL_CUSTOMER_C_BALANCE            (16) /* c_balance    */ 
#define TPCC_COL_CUSTOMER_C_YTD_PAYMENT        (17) /* c_ytd_payment*/ 
#define TPCC_COL_CUSTOMER_C_PAYMENT_CNT        (18) /* c_ytd_payment*/ 
#define TPCC_COL_CUSTOMER_C_DELIVERY_CNT       (19) /* c_ytd_payment*/ 
#define TPCC_COL_CUSTOMER_C_DATA               (20) /* c_data       */ 

#define TPCC_COL_ORDER_LINE_OL_I_ID            (4)
#define TPCC_COL_ORDER_LINE_OL_AMOUNT          (8)

#define TPCC_COL_STOCK_S_I_ID                  (0)
#define TPCC_COL_STOCK_S_W_ID                  (1)
#define TPCC_COL_STOCK_S_QUANTITY              (2)
#define TPCC_COL_STOCK_S_DIST_XX               (3)
#define TPCC_COL_STOCK_S_YTD                   (13)
#define TPCC_COL_STOCK_S_ORDER_CNT             (14)
#define TPCC_COL_STOCK_S_REMOTE_CNT            (15)
#define TPCC_COL_STOCK_S_DATA                  (16)

#define TPCC_COL_ORDER_O_ID                    (2)
#define TPCC_COL_ORDER_O_C_ID                  (3)
#define TPCC_COL_ORDER_O_ENTRY_D               (4)
#define TPCC_COL_ORDER_O_CARRIER_ID            (5)

#define TPCC_COL_ORDER_LINE_OL_I_ID            (4)
#define TPCC_COL_ORDER_LINE_OL_SUPPLY_W_ID     (5)
#define TPCC_COL_ORDER_LINE_OL_DELIVERY_D      (6)
#define TPCC_COL_ORDER_LINE_OL_QUANTITY        (7)
#define TPCC_COL_ORDER_LINE_OL_AMOUNT          (8)

#define TPCC_COL_ITEM_I_ID                     (0)
#define TPCC_COL_ITEM_I_IM_ID                  (1)
#define TPCC_COL_ITEM_I_NAME                   (2)
#define TPCC_COL_ITEM_I_PRICE                  (3)
#define TPCC_COL_ITEM_I_DATA                   (4)

#define TPCC_VAR_W_ID                          (1001)
#define TPCC_VAR_W_NAME                        (1009)
#define TPCC_VAR_W_STREET_1                    (1018)
#define TPCC_VAR_W_STREET_2                    (1019)
#define TPCC_VAR_W_CITY                        (1020)
#define TPCC_VAR_W_STATE                       (1021)
#define TPCC_VAR_W_ZIP                         (1022)
#define TPCC_VAR_H_AMOUNT                      (1002)
#define TPCC_VAR_W_TAX                         (1038)

#define TPCC_VAR_H_KEY                         (1008)
#define TPCC_VAR_O_ID                          (1011)
#define TPCC_VAR_O_CARRIER_ID                  (1012)
#define TPCC_VAR_O_ENTRY_D                     (1035)
#define TPCC_VAR_OL_AMOUNT                     (1013)
#define TPCC_VAR_D_NEXT_O_ID                   (1014)
#define TPCC_VAR_OL_CNT                        (1015)
#define TPCC_VAR_O_ALL_LOCAL                   (1016)
#define TPCC_VAR_NO_ID                         (1017)
#define TPCC_VAR_D_ID                          (1003)
#define TPCC_VAR_D_NAME                        (1010)
#define TPCC_VAR_D_STREET_1                    (1030)
#define TPCC_VAR_D_STREET_2                    (1031)
#define TPCC_VAR_D_CITY                        (1032)
#define TPCC_VAR_D_STATE                       (1033)
#define TPCC_VAR_D_ZIP                         (1034)
#define TPCC_VAR_D_TAX                         (1037)

#define TPCC_VAR_C_LAST                        (1004)
#define TPCC_VAR_C_W_ID                        (1005)
#define TPCC_VAR_C_D_ID                        (1006)
#define TPCC_VAR_C_ID                          (1007)
#define TPCC_VAR_C_STREET_1                    (1040)
#define TPCC_VAR_C_STREET_2                    (1041)
#define TPCC_VAR_C_CITY                        (1042)
#define TPCC_VAR_C_STATE                       (1043)
#define TPCC_VAR_C_ZIP                         (1044)
#define TPCC_VAR_C_PHONE                       (1045)
#define TPCC_VAR_C_SINCE                       (1046)
#define TPCC_VAR_C_CREDIT                      (1047)
#define TPCC_VAR_C_CREDIT_LIM                  (1048)
#define TPCC_VAR_C_DISCOUNT                    (1049)
#define TPCC_VAR_C_BALANCE                     (1050)
#define TPCC_VAR_C_FIRST                       (1051)
#define TPCC_VAR_C_MIDDLE                      (1052)
#define TPCC_VAR_C_ID_LAST                     (1053)
#define TPCC_VAR_THRESHOLD                     (1060)
#define TPCC_VAR_UNKOWN                        (1061)
#define TPCC_VAR_N_PIECE_ALL                   (1080)
#define TPCC_VAR_PIECE_WAREHOUSE               (1071)
#define TPCC_VAR_PIECE_DISTRICT                (1072)

#define TPCC_VAR_I_ID(I)                       (100000 + I)
#define TPCC_VAR_I_NAME(I)                     (101000 + I)
#define TPCC_VAR_I_PRICE(I)                    (102000 + I)
#define TPCC_VAR_I_DATA(I)                     (103000 + I)
#define TPCC_VAR_S_DIST_XX(I)                  (104000 + I)
#define TPCC_VAR_S_DATA(I)                     (105000 + I)
#define TPCC_VAR_S_W_ID(I)                     (106000 + I)
#define TPCC_VAR_S_D_ID(I)                     (107000 + I)
#define TPCC_VAR_OL_QUANTITY(I)                (108000 + I)
#define TPCC_VAR_S_REMOTE_CNT(I)               (109000 + I)
#define TPCC_VAR_OL_D_ID(I)                    (110000 + I)
#define TPCC_VAR_OL_W_ID(I)                    (111000 + I)
#define TPCC_VAR_OL_O_ID(I)                    (112000 + I)
#define TPCC_VAR_OL_NUMBER(I)                  (113000 + I)
#define TPCC_VAR_OL_DELIVER_D(I)               (114000 + I)
#define TPCC_VAR_OL_AMOUNTS(I)                 (115000 + I)
#define TPCC_VAR_OL_DIST_INFO(I)               (116000 + I)
#define TPCC_VAR_OL_I_ID(I)                    (500000 + I)

//
//#define TPCC_NEW_ORDER_Ith_INDEX_ITEM(i)        (5 + 4 * i)
//#define TPCC_NEW_ORDER_Ith_INDEX_IM_STOCK(i)    (6 + 4 * i)
//#define TPCC_NEW_ORDER_Ith_INDEX_DEFER_STOCK(i) (7 + 4 * i)
//#define TPCC_NEW_ORDER_Ith_INDEX_ORDER_LINE(i)  (8 + 4 * i)
//#define TPCC_NEW_ORDER_IS_ITEM_INDEX(i)         ((i >= 5) && (i % 4) == 1)
//#define TPCC_NEW_ORDER_IS_IM_STOCK_INDEX(i)     ((i >= 5) && (i % 4) == 2)
//#define TPCC_NEW_ORDER_INDEX_ITEM_TO_ORDER_LINE(i)  (i + 3)
//#define TPCC_NEW_ORDER_INDEX_ITEM_TO_DEFER_STOCK(i) (i + 2)
//#define TPCC_NEW_ORDER_INDEX_IM_STOCK_TO_ORDER_LINE(i) (i + 2)
//#define TPCC_NEW_ORDER_INDEX_ITEM_TO_CNT(i)     ((i - 5) / 4)
//#define TPCC_NEW_ORDER_INDEX_IM_STOCK_TO_CNT(i) ((i - 6) / 4)

//#define TPCC_NEW_ORDER_RI(i)           (15000+i)
//#define TPCC_NEW_ORDER_RS(i)           (16000+i)
//#define TPCC_NEW_ORDER_WS(i)           (17000+i)
//#define TPCC_NEW_ORDER_WOL(i)          (18000+i)

#define TPCC_NEW_ORDER_Ith_INDEX_ITEM(i)               (15000 + i)
#define TPCC_NEW_ORDER_Ith_INDEX_IM_STOCK(i)           (16000 + i)
#define TPCC_NEW_ORDER_Ith_INDEX_DEFER_STOCK(i)        (17000 + i)
#define TPCC_NEW_ORDER_Ith_INDEX_ORDER_LINE(i)         (18000 + i)
#define TPCC_NEW_ORDER_IS_ITEM_INDEX(i)                ((i >= 15000) && (i < 16000))
#define TPCC_NEW_ORDER_IS_IM_STOCK_INDEX(i)            ((i >= 16000) && (i < 17000))
#define TPCC_NEW_ORDER_INDEX_ITEM_TO_ORDER_LINE(i)     (i + 3000)
#define TPCC_NEW_ORDER_INDEX_ITEM_TO_DEFER_STOCK(i)    (i + 2000)
#define TPCC_NEW_ORDER_INDEX_IM_STOCK_TO_ORDER_LINE(i) (i + 2000)
#define TPCC_NEW_ORDER_INDEX_ITEM_TO_CNT(i)            ((i - 15000))
#define TPCC_NEW_ORDER_INDEX_IM_STOCK_TO_CNT(i)        ((i - 16000))

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


class TpccWorkload: public Workload {
 protected:
  // new order
  virtual void RegNewOrder();

  // payment
  virtual void RegPayment();

  //void reg_payment_remote();
  //void reg_payment_remote_case1();
  //void reg_payment_remote_case2();

  //void reg_payment_home();
  //void reg_payment_home_case1();
  //void reg_payment_home_case2();

  // order status
  virtual void RegOrderStatus();

  // delivery
  virtual void RegDelivery();

  // stock level
  virtual void RegStockLevel();

 public:
  virtual void RegisterPrecedures() override ;

  virtual ~TpccWorkload();

  typedef struct {
    int n_w_id_;
    int n_d_id_;
    int n_c_id_;
    int n_i_id_;
    int const_home_w_id_;
    int delivery_d_id_;
  } tpcc_para_t;
  tpcc_para_t tpcc_para_;

 public:
  TpccWorkload(Config *config);

  // tpcc
  virtual void GetTxRequest(TxRequest* req, uint32_t cid) override;
  // tpcc new_order
  void GetNewOrderTxnReq(TxRequest *req, uint32_t cid) const;
  // tpcc payment
  void get_tpcc_payment_txn_req(TxRequest *req, uint32_t cid) const;
  // tpcc stock_level
  void get_tpcc_stock_level_txn_req(TxRequest *req, uint32_t cid) const;
  // tpcc delivery
  void get_tpcc_delivery_txn_req(TxRequest *req, uint32_t cid) const;
  // tpcc order_status
  void get_tpcc_order_status_txn_req(TxRequest *req, uint32_t cid) const;
};

} // namespace janus

#define C_LAST_SCHEMA (((TpccSharding*)(this->sss_))->g_c_last_schema)
#define C_LAST2ID (((TpccSharding*)(this->sss_))->g_c_last2id)

