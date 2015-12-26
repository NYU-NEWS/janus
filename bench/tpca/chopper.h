#pragma once

#include "coordinator.h"
#include "./bench/tpca/piece.h"

namespace rococo {

class TpcaPaymentChopper: public TxnChopper {

 public:

  TpcaPaymentChopper() {
  }

  virtual void init(TxnRequest &req);

  virtual bool start_callback(const std::vector<int> &pi,
                              int res,
                              BatchStartArgsHelper &bsah) {
    return false;
  }

  virtual bool start_callback(int pi,
                              int res,
                              map<int32_t, Value> &output) { return false; }

  virtual bool is_read_only() { return false; }

  virtual void retry() {
    n_pieces_out_ = 0;
    status_ = {
        {0, READY},
        {1, READY},
        {2, READY}
    };
    commit_.store(true);
    partitions_.clear();
    n_try_++;
  }

  virtual ~TpcaPaymentChopper() { }

};

} // namespace rococo
