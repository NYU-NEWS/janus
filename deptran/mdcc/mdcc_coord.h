//
// Created by lamont on 12/18/15.
//
#ifndef ROCOCO_MDCC_COORD_H
#define ROCOCO_MDCC_COORD_H

#include "deptran/txn_chopper.h"
#include "deptran/three_phase/coord.h"

namespace mdcc {
  using namespace rococo;
  class MdccCoord : public ThreePhaseCoord {
    using ThreePhaseCoord::ThreePhaseCoord;
  public:
    virtual void do_one(TxnRequest&);
    virtual void deptran_start(TxnChopper *ch);
    virtual void deptran_finish(TxnChopper *ch);
  };
}
#endif //ROCOCO_MDCC_COORD_H
