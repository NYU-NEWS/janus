//
// Created by lamont on 12/18/15.
//
#include "coordinator.h"

namespace mdcc {

void MdccCoordinator::do_one(TxnRequest &req) {
  Log_info("MdccCoord::do_one: type=%d", req.txn_type_);
}

void MdccCoordinator::deptran_start(TxnChopper *ch) {
  printf("MdccCoord::deptran_start");
}

void MdccCoordinator::deptran_finish(TxnChopper *ch) {
  printf("MdccCoord::deptran_finish");
}

}
