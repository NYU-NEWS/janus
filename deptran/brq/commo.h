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

  void BroadcastPreAccept(parid_t par_id,
                          txnid_t cmd_id_,
                          ballot_t ballot,
                          RccGraph& graph,
                          const function<void(int, RccGraph&)> &callback)
  {verify(0);}

  void BroadcastCommit(parid_t,
                       txnid_t cmd_id_,
                       RccGraph& graph,
                       const function<void(map<innid_t,
                                               map<int32_t, Value>>&)>
                       &callback) {verify(0);};
};

} // namespace
