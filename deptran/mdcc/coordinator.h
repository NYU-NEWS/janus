//
// Created by lamont on 12/18/15.
//
#ifndef ROCOCO_MDCC_COORD_H
#define ROCOCO_MDCC_COORD_H

#include "deptran/txn_chopper.h"
#include "deptran/three_phase/coord.h"
#include "./communicator.h"

namespace mdcc {
using namespace rococo;
class MdccCoordinator: public ThreePhaseCoordinator {
 public:
  MdccCoordinator(uint32_t coo_id,
                  vector<string> &addrs,
                  int benchmark,
                  int32_t mode,
                  ClientControlServiceImpl *ccsi,
                  uint32_t thread_id,
                  bool batch_optimal) : ThreePhaseCoordinator(coo_id,
                        addrs,
                        benchmark,
                        mode,
                        ccsi,
                        thread_id,
                        batch_optimal) {
    this->commo_ = new MdccCommunicator(addrs);
  }
  virtual void do_one(TxnRequest&);
  virtual void deptran_start(TxnChopper *ch);
  virtual void deptran_finish(TxnChopper *ch);
};
}
#endif //ROCOCO_MDCC_COORD_H
