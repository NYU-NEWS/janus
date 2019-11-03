#pragma once

#include "../__dep__.h"
#include "../executor.h"
#include "../command.h"

namespace janus {

class MultiPaxosExecutor: public Executor {
 public:
  using Executor::Executor;

  /**
   * return max_ballot
   */
  ballot_t Prepare(const ballot_t ballot);

  ballot_t Accept(const ballot_t ballot, shared_ptr<Marshallable> cmd);

  ballot_t Decide(ballot_t ballot, CmdData& cmd);
};

} // namespace janus
