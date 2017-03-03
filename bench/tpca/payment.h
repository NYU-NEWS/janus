#pragma once

#include "coordinator.h"
#include "bench/tpca/workload.h"

namespace rococo {

class TpcaPaymentChopper: public Procedure {

 public:

  TpcaPaymentChopper() {}

  virtual void Init(TxnRequest &req);

  virtual bool start_callback(int pi,
                              int res,
                              map<int32_t, Value> &output) override {
    return false;
  }

  virtual bool IsReadOnly() { return false; }

  virtual void Reset() override;

  virtual ~TpcaPaymentChopper() { }

};

} // namespace rococo
