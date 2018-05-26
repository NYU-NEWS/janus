#pragma once

#include "deptran/workload.h"

namespace janus {

class TmpName;

extern char MICRO_BENCH_TABLE_A[];
extern char MICRO_BENCH_TABLE_B[];
extern char MICRO_BENCH_TABLE_C[];
extern char MICRO_BENCH_TABLE_D[];

#define MICRO_BENCH_R     1
#define MICRO_BENCH_R_NAME "MICRO_R"

#define MICRO_BENCH_R_0   10
#define MICRO_BENCH_R_1   11
#define MICRO_BENCH_R_2   12
#define MICRO_BENCH_R_3   13

#define MICRO_BENCH_W     2
#define MICRO_BENCH_W_NAME "MICRO_W"

#define MICRO_BENCH_W_0   20
#define MICRO_BENCH_W_1   21
#define MICRO_BENCH_W_2   22
#define MICRO_BENCH_W_3   23

#define MICRO_VAR_K_0       (1000)
#define MICRO_VAR_K_1       (1001)
#define MICRO_VAR_K_2       (1002)
#define MICRO_VAR_K_3       (1003)
#define MICRO_VAR_V_0       (2000)
#define MICRO_VAR_V_1       (2001)
#define MICRO_VAR_V_2       (2002)
#define MICRO_VAR_V_3       (2003)

class MicroWorkload : public Workload {
 public:
  using Workload::Workload;
  void RegisterPrecedures() override;
  virtual void GetTxRequest(TxRequest* req, uint32_t cid) override;
  void GetReadReq(TxRequest *req, uint32_t cid) const;
  void GetWriteReq(TxRequest *req, uint32_t cid) const;
};

} // namespace janus

