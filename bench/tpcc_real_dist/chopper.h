#pragma once

#include "../tpcc/chopper.h"

namespace rococo {

class TpccRealDistChopper: public TpccTxn {
 protected:
  virtual siteid_t GetPiecePar(innid_t inn_id);

 public:
  TpccRealDistChopper();
  bool IsOneRound() override;
  virtual void NewOrderInit(TxnRequest &req) override;
  virtual void NewOrderRetry() override;
  virtual void PaymentInit(TxnRequest &req) override;
  virtual void PaymentRetry() override;
  virtual void DeliveryInit(TxnRequest &req) override;
  virtual void DeliveryRetry() override;
  virtual ~TpccRealDistChopper();
};

} // namespace rococo
