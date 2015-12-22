//
// Created by lamont on 12/18/15.
//
#include "coordinator.h"

namespace mdcc {

void MdccCoordinator::do_one(TxnRequest &) {
  printf("MdccCoord::do_one");
}

void MdccCoordinator::deptran_start(TxnChopper *ch) {
  printf("MdccCoord::deptran_start");
}

void MdccCoordinator::deptran_finish(TxnChopper *ch) {
  printf("MdccCoord::deptran_finish");
}

}
