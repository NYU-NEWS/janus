


#include "sched.h"
#include "exec.h"

namespace rococo {

void MultiPaxosSched::OnPrepareRequest(slotid_t slot_id,
                                       ballot_t ballot,
                                       ballot_t* max_ballot,
                                       const function<void()>& cb) {
//  auto exec = (MultiPaxosExecutor*) GetOrCreateExecutor(slot_id);
  Log_debug("Multi-Paxos prepare for slot: %lx", slot_id);
  auto exec = (MultiPaxosExecutor*) CreateExecutor(slot_id);
  *max_ballot = exec->Prepare(ballot);
  cb();
}

void MultiPaxosSched::OnAcceptRequest(const slotid_t slot_id,
                                      const ballot_t ballot,
                                      const Command& cmd,
                                      ballot_t* max_ballot,
                                      const function<void()>& cb) {
  Log_debug("Multi-Paxos accept for slot: %lx", slot_id);
  auto exec = (MultiPaxosExecutor*) GetOrCreateExecutor(slot_id);
  *max_ballot = exec->Accept(ballot, cmd);
  cb();
}

void MultiPaxosSched::OnDecideRequest(const slotid_t slot_id,
                                      const ballot_t ballot,
                                      const Command& cmd) {
  Log_debug("Multi-Paxos decide for slot: %lx", slot_id);
//  verify(0);
}


} // namespace rococo