
#pragma once

#include "../__dep__.h"
#include "../scheduler.h"

namespace rococo {

class SimpleCommand;
class RccGraph;
class RccCommo;
class TxnInfo;
class Vertex;

class RccSched : public Scheduler {
 public:
  RccGraph *dep_graph_;
  RccCommo* commo_;

  int OnHandoutRequest(const SimpleCommand &cmd,
                       rrr::i32 *res,
                       map<int32_t, Value> *output,
                       RccGraph* graph,
                       const function<void()>& callback);

  int OnFinishRequest(cmdid_t cmd_id,
                      const RccGraph& graph,
                      map<int32_t, Value> *output,
                      const function<void()>& callback);

  int OnInquiryRequest(cmdid_t cmd_id,
                       RccGraph* graph,
                       const function<void()>& callback);

  void to_decide(Vertex<TxnInfo> *v,
                 rrr::DeferredReply *defer);

  void send_ask_req(Vertex <TxnInfo> *av);

  RccCommo* commo();
};

} // namespace rococo