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
  ContainerCommand* cmd_ = nullptr;
  ContainerCommand* committed_cmd_ = nullptr;
  /**
   * return max_ballot
   */
  ballot_t Prepare(const ballot_t ballot);

  ballot_t Accept(const ballot_t ballot, const ContainerCommand& cmd);

  ballot_t Decide(ballot_t ballot, ContainerCommand& cmd);
};

} // namespace rococo