#pragma once
#include "../rcc/commo.h"
#include "brq-common.h"

namespace rococo {

class BrqCommo: public RccCommo {
 public:
  void SendHandout(SimpleCommand &cmd,
                   const function<void(int res,
                                       SimpleCommand &cmd,
                                       RccGraph &graph)> &);
  void SendHandoutRo(SimpleCommand &cmd,
                     const function<void(int res,
                                         SimpleCommand &cmd,
                                         map<int, mdb::version_t> &vers)> &)
  override;

  void SendFinish(parid_t pid,
                  txnid_t tid,
                  RccGraph &graph,
                  const function<void(int res, map<innid_t,
                                                   map<int32_t, Value>> &
                  output)> &callback) override;

  void SendInquire(parid_t pid,
                   txnid_t tid,
                   const function<void(RccGraph &graph)> &) override;
};

} // namespace
