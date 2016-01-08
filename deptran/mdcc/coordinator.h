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
  uint32_t site_id_;
  uint32_t thread_id_;
  Config* config_;
  ClientControlServiceImpl* ccsi_;

public:
  MdccCoordinator() = delete;
  MdccCoordinator(uint32_t site_id,
                  uint32_t thread_id,
                  Config* config,
                  ClientControlServiceImpl *ccsi) :
    communicator_(new MdccCommunicator(config, site_id)),
    site_id_(site_id),
    thread_id_(thread_id),
    config_(config),
    ccsi_(ccsi) {
  }

  virtual ~MdccCoordinator() { delete communicator_; }
  virtual void do_one(TxnRequest&) override;
  void cleanup() override {}
  void restart(TxnChopper *ch) override {}

  i64 NextTxnId();
};
}
#endif //ROCOCO_MDCC_COORD_H
