//
// Created by lamont on 12/18/15.
//

#include "mdcc_coord.h"

namespace mdcc {
  void MdccCoord::do_one(TxnRequest &) {
    printf("MdccCoord::do_one");
  }

  void MdccCoord::deptran_start(TxnChopper *ch) {
    printf("MdccCoord::deptran_start");
  }

  void MdccCoord::deptran_finish(TxnChopper *ch) {
    printf("MdccCoord::deptran_finish");
  }
}
