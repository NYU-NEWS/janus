#pragma once

#include "__dep__.h"
#include "constants.h"
#include "../rcc/graph.h"
#include "../rcc/graph_marshaler.h"
#include "../command.h"
#include "../txn_chopper.h"
#include "../command_marshaler.h"
#include "../rcc_rpc.h"

class SimpleCommand;
namespace rococo {

class Scheduler;
class MultiPaxosSched;
class MultiPaxosServiceImpl : public MultiPaxosService {
 public:
  MultiPaxosSched* sched_;
  MultiPaxosServiceImpl(Scheduler* sched);
  void Forward(const ContainerCommand& cmd,
               rrr::DeferredReply* defer) override;

  void Prepare(const uint64_t& slot,
               const ballot_t& ballot,
               uint64_t* max_ballot,
               rrr::DeferredReply* defer) override;

  void Accept(const uint64_t& slot,
              const ballot_t& ballot,
              const ContainerCommand& cmd,
              uint64_t* max_ballot,
              rrr::DeferredReply* defer) override;

  void Decide(const uint64_t& slot,
              const ballot_t& ballot,
              const ContainerCommand& cmd,
              rrr::DeferredReply* defer) override;

};

} // namespace rococo
