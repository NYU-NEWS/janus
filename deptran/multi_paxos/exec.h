#pragma once

#include "../__dep__.h"
#include "../executor.h"
#include "../command.h"

namespace rococo {

class MultiPaxosExecutor: public Executor {
 public:
  using Executor::Executor;

  ballot_t max_ballot_seen_ = 0;
  ballot_t max_ballot_accepted_ = 0;
  SimpleCommand cmd_;
  /**
   * return max_ballot
   */
  ballot_t Prepare(ballot_t ballot);

  ballot_t Accept(ballot_t ballot, SimpleCommand& cmd);

  ballot_t Decide(ballot_t ballot, SimpleCommand& cmd);
};

} // namespace rococo