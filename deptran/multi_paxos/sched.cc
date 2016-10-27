


#include "sched.h"
#include "exec.h"

namespace rococo {

void MultiPaxosSched::OnPrepare(slotid_t slot_id,
                                ballot_t ballot,
                                ballot_t *max_ballot,
                                const function<void()> &cb) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  Log_debug("multi-paxos scheduler receives prepare for slot_id: %llx",
            slot_id);
  auto exec = (MultiPaxosExecutor*) CreateExecutor(slot_id);
  *max_ballot = exec->Prepare(ballot);
  cb();
  verify(0); // for debug.
}

void MultiPaxosSched::OnAccept(const slotid_t slot_id,
                               const ballot_t ballot,
                               const ContainerCommand &cmd,
                               ballot_t *max_ballot,
                               const function<void()> &cb) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  Log_debug("multi-paxos scheduler accept for slot_id: %llx", slot_id);
  auto exec = (MultiPaxosExecutor*) GetOrCreateExecutor(slot_id);
  verify(exec != nullptr);
  *max_ballot = exec->Accept(ballot, cmd);
  cb();
}

void MultiPaxosSched::OnCommit(const slotid_t slot_id,
                               const ballot_t ballot,
                               const ContainerCommand &cmd) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  Log_debug("multi-paxos scheduler decide for slot: %lx", slot_id);
  if (slot_id > max_committed_slot_) {
    max_committed_slot_ = slot_id;
  }
  verify(slot_id > max_executed_slot_);
  if (slot_id == max_executed_slot_ + 1) {
    max_executed_slot_++;
    learner_action_(const_cast<ContainerCommand&>(cmd));
    Log_debug("execute slot %d now", slot_id);
    // TODO check later cmds
    for (slotid_t next_slot = slot_id + 1;
        next_slot <= max_committed_slot_;
        next_slot++) {
      auto exec = (MultiPaxosExecutor*) GetOrCreateExecutor(next_slot);
      if (exec->committed_cmd_) {
        Log_debug("execute slot %d now", next_slot);
        learner_action_(*exec->committed_cmd_);
        max_executed_slot_++;
      } else {
        break;
      }
    }
  } else {
    // remember commands for later execution.
    auto exec = (MultiPaxosExecutor*) GetOrCreateExecutor(slot_id);
    exec->committed_cmd_ = cmd.Clone();
    Log_debug("cannot execute slot %d now, remember to exec later. max_exe: %d",
             (int)slot_id, (int)max_executed_slot_);
  }
}


} // namespace rococo