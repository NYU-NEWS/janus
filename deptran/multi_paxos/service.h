#pragma once

#include "__dep__.h"
#include "constants.h"
#include "../rcc/graph.h"
#include "../rcc/graph_marshaler.h"
#include "../command.h"
#include "../command_marshaler.h"
#include "../rcc_rpc.h"

class SimpleCommand;
namespace rococo {

class MultiPaxosServiceImpl : public MultiPaxosService {
 public:

  void Forward(const SimpleCommand& cmd, rrr::DeferredReply* defer) override;

  void Prepare(const uint64_t& slot,
               const ballot_t& ballot,
               uint64_t* max_ballot,
               rrr::DeferredReply* defer) override;

  void Accept(const uint64_t& slot,
              const ballot_t& ballot,
              const SimpleCommand& cmd,
              uint64_t* max_ballot,
              rrr::DeferredReply* defer) override;

  void Decide(const uint64_t& slot,
              const ballot_t& ballot,
              const SimpleCommand& cmd,
              rrr::DeferredReply* defer) override;


};

} // namespace rococo