


#include "sched.h"
#include "exec.h"

namespace rococo {

void MultiPaxosSched::OnPrepare(slotid_t slot_id,
                                ballot_t ballot,
                                ballot_t *max_ballot,
                                const function<void()> &cb) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
//  auto exec = (MultiPaxosExecutor*) GetOrCreateExecutor(slot_id);
  Log_debug("multi-paxos scheduler receives prepare for slot_id: %llx",
            slot_id);
  auto exec = (MultiPaxosExecutor*) CreateExecutor(slot_id);
  *max_ballot = exec->Prepare(ballot);
  cb();
}

void MultiPaxosSched::OnAccept(const slotid_t slot_id,
                               const ballot_t ballot,
                               const ContainerCommand &cmd,
                               ballot_t *max_ballot,
                               const function<void()> &cb) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  Log_debug("multi-paxos scheduler accept for slot_id: %llx", slot_id);
  auto exec = (MultiPaxosExecutor*) GetExecutor(slot_id);
  verify(exec != nullptr);
  *max_ballot = exec->Accept(ballot, cmd);
  cb();
}

void MultiPaxosSched::OnCommit(const slotid_t slot_id,
                               const ballot_t ballot,
                               const ContainerCommand &cmd) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  Log_debug("multi-paxos scheduler decide for slot: %lx", slot_id);
  learner_action_(const_cast<ContainerCommand&>(cmd));
//  verify(0);
}


} // namespace rococo