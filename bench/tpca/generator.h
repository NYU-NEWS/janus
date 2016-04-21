#pragma once

#include "deptran/txn_req_factory.h"

namespace rococo {

class TpcaTxnGenerator: public TxnGenerator {
 public:
//  using TxnGenerator::TxnGenerator;
  TpcaTxnGenerator(Config* config);
  virtual void GetTxnReq(TxnRequest *req, uint32_t cid) const override;
};

} // namespace rococo