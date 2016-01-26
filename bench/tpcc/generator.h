
#pragma once

#include <deptran/config.h>
#include "__dep__.h"
#include "deptran/txn_req_factory.h"

namespace rococo {

class TpccTxnGenerator : public TxnGenerator {

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
  using TxnGenerator::TxnGenerator;
  TpccTxnGenerator(rococo::Config *config);

  // tpcc
  virtual void get_txn_req(TxnRequest *req, uint32_t cid) const;
  // tpcc new_order
  void get_tpcc_new_order_txn_req(TxnRequest *req, uint32_t cid) const;
  // tpcc payment
  void get_tpcc_payment_txn_req(TxnRequest *req, uint32_t cid) const;
  // tpcc stock_level
  void get_tpcc_stock_level_txn_req(TxnRequest *req, uint32_t cid) const;
  // tpcc delivery
  void get_tpcc_delivery_txn_req(TxnRequest *req, uint32_t cid) const;
  // tpcc order_status
  void get_tpcc_order_status_txn_req(TxnRequest *req, uint32_t cid) const;
};

} // namespace rococo

