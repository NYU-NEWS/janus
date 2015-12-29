//
// Created by lamont on 12/28/15.
//
#pragma once
#include "deptran/txn_chopper.h"
#include <map>
#include <vector>

namespace rococo {
  class SimpleBenchChopper : public TxnChopper {
    virtual void init(TxnRequest &req);
    virtual bool start_callback(const std::vector<int> &pi,
                                int res,
                                BatchStartArgsHelper &bsah) { return false; };
    virtual bool start_callback(int pi,
                                int res,
                                map<int32_t, mdb::Value> &output);
    virtual bool start_callback(int pi,
                                ChopStartResponse &res);
    virtual bool start_callback(int pi,
                                map<int32_t, mdb::Value> &output,
                                bool is_defer);
    virtual bool finish_callback(ChopFinishResponse &res) { return false; }
    virtual bool is_read_only() { return false; }
    virtual void retry() { };
  };
}
