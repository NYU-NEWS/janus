#pragma once

#include "__dep__.h"
#include "constants.h"
#include "../rococo/graph.h"
#include "../rococo/graph_marshaler.h"
#include "../command.h"
#include "deptran/procedure.h"
#include "../command_marshaler.h"
#include "../rcc_rpc.h"

class SimpleCommand;
namespace janus {

class Scheduler;
class SchedulerMultiPaxos;
class MultiPaxosServiceImpl : public MultiPaxosService {
 public:
  SchedulerMultiPaxos* sched_;
  MultiPaxosServiceImpl(Scheduler* sched);
  void Forward(const MarshallDeputy& cmd,
               rrr::DeferredReply* defer) override;

  void Prepare(const uint64_t& slot,
               const ballot_t& ballot,
               ballot_t* max_ballot,
               rrr::DeferredReply* defer) override;

  void Accept(const uint64_t& slot,
              const ballot_t& ballot,
              const MarshallDeputy& cmd,
              ballot_t* max_ballot,
              rrr::DeferredReply* defer) override;

  void Decide(const uint64_t& slot,
              const ballot_t& ballot,
              const MarshallDeputy& cmd,
              rrr::DeferredReply* defer) override;

};

} // namespace janus
