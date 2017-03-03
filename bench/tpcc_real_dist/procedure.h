#pragma once

#include "bench/tpcc/procedure.h"

namespace rococo {

class TpccRdProcedure: public TpccProcedure {
 protected:
  virtual siteid_t GetPiecePar(innid_t inn_id);

 public:
  TpccRdProcedure();
  bool IsOneRound() override;
  virtual void NewOrderInit(TxnRequest &req) override;
  virtual void NewOrderRetry() override;
  virtual void PaymentInit(TxnRequest &req) override;
  virtual void PaymentRetry() override;
  virtual void DeliveryInit(TxnRequest &req) override;
  virtual void DeliveryRetry() override;
  virtual ~TpccRdProcedure();
};

} // namespace rococo
