#pragma once

#include "../three_phase/coord.h"

namespace rococo {
class RCCCoord : public ThreePhaseCoord {
  using ThreePhaseCoord::ThreePhaseCoord;

public:

  struct deptran_batch_start_t {
    std::vector<RequestHeader>      headers;
    std::vector<std::vector<Value> >inputs;
    std::vector<int>                output_sizes;
    std::vector<int>                pis;
    rrr::FutureAttr                 fuattr;
  };

  virtual void do_one(TxnRequest&);

  virtual void deptran_start(TxnChopper *ch);

  void         deptran_batch_start(TxnChopper *ch);

  virtual void deptran_finish(TxnChopper *ch);

  void         deptran_start_ro(TxnChopper *ch);

  void         deptran_finish_ro(TxnChopper *ch);
};
} // namespace rococo
