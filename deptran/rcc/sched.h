
#pragma once

#include "../__dep__.h"
#include "../scheduler.h"

namespace rococo {

class SimpleCommand;
class RccGraph;

class RccSched : public Scheduler {
 public:
  RccGraph *dep_graph_;

  int OnHandoutRequest(const SimpleCommand &cmd,
                       rrr::i32 *res,
                       map<int32_t, Value> *output,
                       RccGraph* graph,
                       const function<void()>& callback);
};

} // namespace rococo