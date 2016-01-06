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
class MdccCoordinator: public CoordinatorBase {
protected:
  MdccCommunicator* communicator_ = nullptr;

public:
  MdccCoordinator(uint32_t coo_id,
                  Config* config,
                  int benchmark,
                  int32_t mode,
                  ClientControlServiceImpl *ccsi,
                  uint32_t thread_id,
                  bool batch_optimal) {
    this->communicator_ = new MdccCommunicator(config);
  }
  virtual ~MdccCoordinator() { delete communicator_; }
  virtual void do_one(TxnRequest&);
  void cleanup() override {}
  void restart(TxnChopper *ch) override {}

  i64 NextTxnId();
};
}
#endif //ROCOCO_MDCC_COORD_H
