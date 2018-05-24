#include "exec.h"

namespace rococo {


ballot_t MultiPaxosExecutor::Prepare(const ballot_t ballot) {
}

ballot_t MultiPaxosExecutor::Accept(const ballot_t ballot,
                                    shared_ptr<Marshallable> cmd) {
}

ballot_t MultiPaxosExecutor::Decide(ballot_t ballot, CmdData& cmd) {
}

} // namespace rococo