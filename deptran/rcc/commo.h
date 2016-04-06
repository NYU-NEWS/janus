#pragma once
#include "../__dep__.h"
#include "../communicator.h"

namespace rococo {

class RccGraph;
class RccCommo : public Communicator {
 public:
  using Communicator::Communicator;
  virtual void SendHandout(SimpleCommand &cmd,
                   const function<void(int res,
                                       SimpleCommand& cmd,
                                       RccGraph& graph)>&) ;
  virtual void SendHandoutRo(SimpleCommand &cmd,
                     const function<void(int res,
                                         SimpleCommand& cmd,
                                         map<int, mdb::version_t>& vers)>&) ;

  virtual void SendFinish(parid_t pid,
                  txnid_t tid,
                  RccGraph& graph,
                  const function<void(map<innid_t,
                                                   map<int32_t, Value>>&
                  output)> &callback) ;

  virtual void SendInquire(parid_t pid,
                   txnid_t tid,
                   const function<void(RccGraph& graph)>&);
};

} // namespace rococo