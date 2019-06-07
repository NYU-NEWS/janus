#include "exec.h"

namespace janus {


ballot_t MultiPaxosExecutor::Prepare(const ballot_t ballot) {
  verify(0);
  return 0;
}

ballot_t MultiPaxosExecutor::Accept(const ballot_t ballot,
                                    shared_ptr<Marshallable> cmd) {
  verify(0);
  return 0;
}

ballot_t MultiPaxosExecutor::Decide(ballot_t ballot, CmdData& cmd) {
  verify(0);
  return 0;
}

} // namespace janus
