//
// Created by lamont on 12/18/15.
//
#ifndef ROCOCO_MDCC_COORD_H
#define ROCOCO_MDCC_COORD_H

#include "deptran/txn_chopper.h"
#include "deptran/classic/coord.h"
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
  std::atomic<uint32_t> txn_id_counter_;

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
    ccsi_(ccsi),
    txn_id_counter_(0) {
  }

  virtual ~MdccCoordinator() { delete communicator_; }
  virtual void do_one(TxnRequest&) override;
  void Reset() override {}
  void restart(TxnCommand *ch) override {}

  uint64_t NextTxnId();
};
}
#endif //ROCOCO_MDCC_COORD_H
