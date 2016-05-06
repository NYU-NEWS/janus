//
// Created by lamont on 5/6/16.
//
#pragma once

#include "deptran/__dep__.h"
#include "deptran/txn_req_factory.h"
namespace rococo {
  class RWTxnGenerator : public TxnGenerator {
  public:
    map<cooid_t, int32_t> key_ids_ = {};
    RWTxnGenerator(Config *config);
    virtual void GetTxnReq(TxnRequest *req, uint32_t cid) override;

  protected:
    int32_t GetId(uint32_t cid);
    void GenerateWriteRequest(TxnRequest *req, uint32_t cid);
    void GenerateReadRequest(TxnRequest *req, uint32_t cid);
  };
}
