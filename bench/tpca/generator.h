#pragma once

#include "deptran/txn_req_factory.h"

namespace rococo {

class TpcaTxnGenerator: public TxnGenerator {
 public:
  map<int32_t, int32_t> key_ids_ = {};
  TpcaTxnGenerator(Config* config);
  virtual void GetTxnReq(TxnRequest *req, uint32_t cid) override;
};

} // namespace rococo