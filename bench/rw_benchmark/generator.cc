//
// Created by lamont on 5/6/16.
//

#include <deptran/txn_chopper.h>
#include "generator.h"
#include "piece.h"

namespace rococo {

RWTxnGenerator::RWTxnGenerator(rococo::Config *config) : TxnGenerator(config) {
}

void RWTxnGenerator::GetTxnReq(TxnRequest *req, uint32_t cid) {
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

void RWTxnGenerator::GenerateWriteRequest(
    TxnRequest *req, uint32_t cid) {
  auto id = this->GetId(cid);
  req->txn_type_ = RW_BENCHMARK_W_TXN;
  req->input_ = {
      {0, Value((i32) id)},
      {1, Value((i32) RandomGenerator::rand(0, 10000))}
  };
}

void RWTxnGenerator::GenerateReadRequest(
    TxnRequest *req, uint32_t cid) {
  auto id = GetId(cid);
  req->txn_type_ = RW_BENCHMARK_R_TXN;
  req->input_ = {
      {0, Value((i32) id)}
  };
}

int32_t RWTxnGenerator::GetId(uint32_t cid) {
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
}
