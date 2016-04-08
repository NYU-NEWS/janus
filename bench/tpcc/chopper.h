#ifndef TPCC_CHOPPER_H_
#define TPCC_CHOPPER_H_

#include "memdb/value.h"
#include <vector>
#include "./bench/tpcc/piece.h"

namespace rococo {

class TxnCommand;
class TxnRequest;
class BatchStartArgsHelper;

class TpccTxn: public TxnCommand {
 public:
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

  virtual void NewOrderInit(TxnRequest &req);

  virtual void NewOrderRetry();

//  // payment
//  virtual void payment_shard(const char *tb,
//                             map<int32_t, Value> &input,
//                             uint32_t &site);

  virtual void PaymentInit(TxnRequest &req);

  virtual void PaymentRetry();

  virtual void StockLevelInit(TxnRequest &req);

  virtual void StockLevelRetry();

  // delivery
  virtual void DeliveryInit(TxnRequest &req);

  virtual void DeliveryRetry();

  // order status
//  virtual void order_status_shard(const char *tb,
//                                  map<int32_t, Value> &input,
//                                  uint32_t &site);

  virtual void OrderStatusInit(TxnRequest &req);

  virtual void OrderStatusRetry();

 public:
  TpccTxn();

  virtual parid_t GetPiecePartitionId(innid_t inn_id);

  virtual void Init(TxnRequest &req);
  virtual bool start_callback(int pi,
                              int res,
                              map<int32_t, Value> &output);
  virtual bool IsReadOnly();

  virtual void Reset();

  virtual bool CheckReady();

  virtual int GetNPieceAll();

  virtual ~TpccTxn();
};

}

#endif // TPCC_CHOPPER_H_
