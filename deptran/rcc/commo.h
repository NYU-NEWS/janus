#pragma once
#include "../__dep__.h"
#include "../communicator.h"

namespace rococo {

class RccGraph;
class RccCommo : public Communicator {
 public:
  void SendHandout(SimpleCommand &cmd,
                   const function<void(int res,
                                       SimpleCommand& cmd,
                                       RccGraph& graph)>&) ;
  void SendHandoutRo(SimpleCommand &cmd,
                     const function<void(int res,
                                         SimpleCommand& cmd,
                                         map<int, mdb::version_t>& vers)>&) ;

  void SendFinish(parid_t pid,
                  txnid_t tid,
                  RccGraph& graph,
                  const function<void(int res, map<innid_t,
                                                   map<int32_t, Value>>&
                  output)> &callback) ;

  void SendInquire(parid_t pid,
                   txnid_t tid,
                   const function<void(RccGraph& graph)>&);
};

} // namespace rococo