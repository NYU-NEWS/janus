#include "exec.h"

namespace rococo {


ballot_t MultiPaxosExecutor::Prepare(const ballot_t ballot) {
  verify(ballot != max_ballot_seen_);
  if (max_ballot_seen_ < ballot) {
    max_ballot_seen_ = ballot;
  } else {
    // TODO if accepted anything, return;
    verify(0);
  }
  return max_ballot_seen_;
}

ballot_t MultiPaxosExecutor::Accept(const ballot_t ballot,
                                    const ContainerCommand& cmd) {
  verify(max_ballot_accepted_ < ballot);
  if (max_ballot_seen_ <= ballot) {
    max_ballot_seen_ = ballot;
    max_ballot_accepted_ = ballot;
  } else {
    // TODO
    verify(0);
  }
  return max_ballot_seen_;
}

ballot_t MultiPaxosExecutor::Decide(ballot_t ballot, ContainerCommand& cmd) {
  verify(max_ballot_seen_ <= ballot);
  max_ballot_seen_ = ballot;
  max_ballot_accepted_ = ballot;
}

} // namespace rococo