#pragma once
#include "deptran/__dep__.h"
#include "deptran/txn_reg.h"
#include "bench/tpcc/workload.h"

namespace janus {

class TxData;
class TxRequest;
class BatchStartArgsHelper;

class TpccProcedure: public TxData {
 public:
  map<innid_t, TxnPieceDef> leaf_procs_{};
//  map<innid_t, set<int32_t>> input_vars_{};
  typedef struct {
    size_t ol_cnt;
    bool piece_0_dist;
    bool *piece_items;
    bool *piece_stocks;
  } new_order_dep_t;

  typedef struct {
    bool piece_warehouse;
    bool piece_district;
    bool piece_last2id;
    bool piece_ori_last2id;
  } payment_dep_t;

  typedef struct {
    bool piece_last2id;
    bool piece_ori_last2id;
    bool piece_order;
  } order_status_dep_t;

  //typedef struct {
  //    size_t d_cnt;
  //    bool *piece_new_orders;
  //    bool *piece_orders;
  //    bool *piece_order_lines;
  //} delivery_dep_t;
  typedef struct {
    bool piece_new_orders;
    bool piece_orders;
    bool piece_order_lines;
  } delivery_dep_t;

  typedef struct {
    int threshold;
    int w_id;
  } stock_level_dep_t;

  union {
    new_order_dep_t new_order_dep_;
    payment_dep_t payment_dep_;
    order_status_dep_t order_status_dep_;
    delivery_dep_t delivery_dep_;
    stock_level_dep_t stock_level_dep_;
  };

  virtual void NewOrderInit(TxRequest &req);

  virtual void NewOrderRetry();

//  // payment
//  virtual void payment_shard(const char *tb,
//                             map<int32_t, Value> &input,
//                             uint32_t &site);

  virtual void PaymentInit(TxRequest &req);

  virtual void PaymentRetry();

  virtual void StockLevelInit(TxRequest &req);

  virtual void StockLevelRetry();

  // delivery
  virtual void DeliveryInit(TxRequest &req);

  virtual void DeliveryRetry();

  // order status
//  virtual void order_status_shard(const char *tb,
//                                  map<int32_t, Value> &input,
//                                  uint32_t &site);

  virtual void OrderStatusInit(TxRequest &req);

  virtual void OrderStatusRetry();

 public:
  TpccProcedure();

  virtual parid_t GetPiecePartitionId(innid_t inn_id) override ;

  virtual void Init(TxRequest &req) override ;
  virtual bool HandleOutput(int pi,
                            int res,
                            map<int32_t, Value> &output) override;
  virtual bool IsReadOnly() override ;

  virtual void Reset() override ;

  virtual bool CheckReady();

  virtual int GetNPieceAll() override ;

  virtual ~TpccProcedure();
};

}
