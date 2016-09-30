#pragma once

#include "deptran/__dep__.h"
#include "deptran/txn_req_factory.h"

namespace rococo {

class TpcaTxnGenerator: public TxnGenerator {
 public:
//  static set<int32_t> __debug_key_test_{};

  boost::random::mt19937 rand_gen_;
  map<int32_t, int32_t> key_ids_ = {};
  TpcaTxnGenerator(Config* config);
  virtual void GetTxnReq(TxnRequest *req, uint32_t cid) override;
  virtual void GetTxnReq(TxnRequest *req,
                         uint32_t i_client,
                         uint32_t n_client) override;
};

} // namespace rococo