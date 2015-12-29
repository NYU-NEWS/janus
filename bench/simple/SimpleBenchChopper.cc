//
// Created by lamont on 12/28/15.
//
#include "SimpleBenchChopper.h"

namespace rococo {
  void SimpleBenchChopper::init(TxnRequest &req) {
  }

  bool SimpleBenchChopper::start_callback(int pi,
                                          int res,
                                          map<int32_t, Value> &output) {
    return false;
  }

  bool SimpleBenchChopper::start_callback(int pi,
                                          ChopStartResponse &res) {
    return false;
  }

  bool SimpleBenchChopper::start_callback(int pi,
                                          map<int32_t, mdb::Value> &output,
                                          bool is_defer) {
    return false;
  }
}