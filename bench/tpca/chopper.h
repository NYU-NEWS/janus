#pragma once

#include "coordinator.h"
#include "./bench/tpca/piece.h"

namespace rococo {

class TpcaPaymentChopper: public TxnCommand {

 public:

  TpcaPaymentChopper() {}

  virtual void Init(TxnRequest &req);

  virtual bool start_callback(const std::vector<int> &pi,
                              int res,
                              BatchStartArgsHelper &bsah) {
    return false;
  }

  virtual bool start_callback(int pi,
                              int res,
                              map<int32_t, Value> &output) { return false; }

  virtual bool IsReadOnly() { return false; }

  virtual void Reset() override;

  virtual ~TpcaPaymentChopper() { }

};

} // namespace rococo
