#pragma once

#include "../three_phase/coord.h"

namespace rococo {
class RCCCoord : public ThreePhaseCoordinator {

protected:
  inline RococoCommunicator* comm() {
    return static_cast<RococoCommunicator*>(commo_);
  }

public:
  RCCCoord(uint32_t coo_id, vector<string> &addrs, int benchmark, int32_t mode, ClientControlServiceImpl *ccsi,
           uint32_t thread_id, bool batch_optimal) : ThreePhaseCoordinator(coo_id, addrs, benchmark, mode, ccsi,
                                                                           thread_id, batch_optimal) {
  }

  struct deptran_batch_start_t {
    std::vector<RequestHeader>      headers;
    std::vector<map<int32_t, Value> >inputs;
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
