


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
}

void MultiPaxosSched::OnAcceptRequest(slotid_t slot_id,
                                      ballot_t ballot,
                                      ballot_t* max_ballot,
                                      const function<void()>& cb) {
  verify(0);
}

void MultiPaxosSched::OnDecideRequest(slotid_t slot_id,
                                      ballot_t ballot) {
  verify(0);
}


} // namespace rococo