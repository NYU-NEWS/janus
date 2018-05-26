#pragma once

#include "deptran/workload.h"

namespace janus {

extern char TPCA_BRANCH[];
extern char TPCA_TELLER[];
extern char TPCA_CUSTOMER[];

#define TPCA_PAYMENT (10)
#define TPCA_PAYMENT_NAME "PAYMENT"

#define TPCA_PAYMENT_1 (11)
#define TPCA_PAYMENT_2 (12)
#define TPCA_PAYMENT_3 (13)

#define TPCA_ROW_1 (21)
#define TPCA_ROW_2 (22)
#define TPCA_ROW_3 (23)

#define TPCA_VAR_X   (0)
#define TPCA_VAR_Y   (1)
#define TPCA_VAR_Z   (2)
#define TPCA_VAR_OX  (3)
#define TPCA_VAR_OY  (4)
#define TPCA_VAR_OZ  (5)
#define TPCA_VAR_AMOUNT    (10)

class TpcaWorkload : public Workload {
 public:
  void RegisterPrecedures() override;
  std::mt19937 rand_gen_{};
  map<int32_t, int32_t> key_ids_{};
  TpcaWorkload(Config* config);
  virtual void GetTxRequest(TxRequest* req, uint32_t cid) override;
//  virtual void GetTxRequest(TxnRequest *req,
//                         uint32_t i_client,
//                         uint32_t n_client) override;
};

} // namespace janus
