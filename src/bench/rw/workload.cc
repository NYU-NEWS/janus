#include "deptran/__dep__.h"
#include "bench/tpcc/workload.h"
#include "workload.h"

namespace janus {
char RW_BENCHMARK_TABLE[] = "history";

void RwWorkload::RegisterPrecedures() {
  RegP(RW_BENCHMARK_R_TXN, RW_BENCHMARK_R_TXN_0,
       {}, // TODO i
       {}, // TODO o
       {}, // TODO c
       {TPCC_TB_HISTORY, {TPCC_VAR_H_KEY}}, // s
       DF_NO,
       PROC {
           mdb::MultiBlob buf(1);
           Value result(0);
           verify(cmd.input.size() == 1);
           auto id = cmd.input[0].get_i64();
           buf[0] = cmd.input[0].get_blob();
           auto tbl = tx.GetTable(RW_BENCHMARK_TABLE);
           auto row = tx.Query(tbl, buf);
           tx.ReadColumn(row, 1, &result, TXN_BYPASS);
           output[0] = result;
           *res = SUCCESS;
           return;
       }
  );

  RegP(RW_BENCHMARK_W_TXN, RW_BENCHMARK_W_TXN_0,
       {}, // TODO i
       {}, // TODO o
       {}, // TODO c
       {TPCC_TB_HISTORY, {TPCC_VAR_H_KEY}}, // s
       DF_REAL,
       PROC {
         mdb::MultiBlob buf(1);
         Value result(0);
         verify(cmd.input.size() == 1);
         auto id = cmd.input[0].get_i64();
         buf[0] = cmd.input[0].get_blob();
         auto tbl = tx.GetTable(RW_BENCHMARK_TABLE);
         auto row = tx.Query(tbl, buf);
         tx.ReadColumn(row, 1, &result, TXN_BYPASS);
         result.set_i32(result.get_i32() + 1);
         tx.WriteColumn(row, 1, result, TXN_DEFERRED);
         *res = SUCCESS;
         return;
       }
  );
}

RwWorkload::RwWorkload(Config *config) : Workload(config) {
}

void RwWorkload::GetTxRequest(TxRequest* req, uint32_t cid) {
  req->n_try_ = n_try_;
  std::vector<double> weights = {txn_weights_["read"], txn_weights_["write"]};
  switch (RandomGenerator::weighted_select(weights)) {
    case 0: // read
      GenerateReadRequest(req, cid);
      break;
    case 1: // write
      GenerateWriteRequest(req, cid);
      break;
    default:
      verify(0);
  }
}

void RwWorkload::GenerateWriteRequest(
    TxRequest *req, uint32_t cid) {
  auto id = this->GetId(cid);
  req->tx_type_ = RW_BENCHMARK_W_TXN;
  req->input_ = {
      {0, Value((i32) id)},
      {1, Value((i32) RandomGenerator::rand(0, 10000))}
  };
}

void RwWorkload::GenerateReadRequest(
    TxRequest *req, uint32_t cid) {
  auto id = GetId(cid);
  req->tx_type_ = RW_BENCHMARK_R_TXN;
  req->input_ = {
      {0, Value((i32) id)}
  };
}

int32_t RwWorkload::GetId(uint32_t cid) {
  int32_t id;
  if (fix_id_ == -1) {
    id = RandomGenerator::rand(0, rw_benchmark_para_.n_table_ - 1);
  } else {
    auto it = this->key_ids_.find(cid);
    if (it == key_ids_.end()) {
      id = fix_id_;
      id += (cid & 0xFFFFFFFF);
      id = (id<0) ? -1*id : id;
      id %= rw_benchmark_para_.n_table_;
      key_ids_[cid] = id;
      Log_info("coordinator %d using fixed id of %d", cid, id);
    } else {
      id = it->second;
    }
  }
  return id;
}
} // namespace janus

